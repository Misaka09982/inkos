import type { LLMClient } from "../llm/provider.js";
import type { BookConfig } from "../models/book.js";
import type { ChapterMeta } from "../models/chapter.js";
import type { NotifyChannel } from "../models/project.js";
import type { GenreProfile } from "../models/genre-profile.js";
import { ArchitectAgent } from "../agents/architect.js";
import { WriterAgent } from "../agents/writer.js";
import { ContinuityAuditor } from "../agents/continuity.js";
import { ReviserAgent, type ReviseMode } from "../agents/reviser.js";
import { RadarAgent } from "../agents/radar.js";
import type { RadarSource } from "../agents/radar-source.js";
import { readGenreProfile } from "../agents/rules-reader.js";
import { analyzeAITells } from "../agents/ai-tells.js";
import { analyzeSensitiveWords } from "../agents/sensitive-words.js";
import { StateManager } from "../state/manager.js";
import { dispatchNotification, dispatchWebhookEvent } from "../notify/dispatcher.js";
import type { WebhookEvent } from "../notify/webhook.js";
import type { AgentContext } from "../agents/base.js";
import type { AuditResult, AuditIssue } from "../agents/continuity.js";
import type { RadarResult } from "../agents/radar.js";
import { readFile, readdir, writeFile } from "node:fs/promises";
import { join } from "node:path";

export interface PipelineConfig {
  readonly client: LLMClient;
  readonly model: string;
  readonly projectRoot: string;
  readonly notifyChannels?: ReadonlyArray<NotifyChannel>;
  readonly radarSources?: ReadonlyArray<RadarSource>;
  readonly externalContext?: string;
  readonly modelOverrides?: Record<string, string>;
}

export interface ChapterPipelineResult {
  readonly chapterNumber: number;
  readonly title: string;
  readonly wordCount: number;
  readonly auditResult: AuditResult;
  readonly revised: boolean;
  readonly status: "approved" | "needs-review";
}

// Atomic operation results
export interface DraftResult {
  readonly chapterNumber: number;
  readonly title: string;
  readonly wordCount: number;
  readonly filePath: string;
}

export interface ReviseResult {
  readonly chapterNumber: number;
  readonly wordCount: number;
  readonly fixedIssues: ReadonlyArray<string>;
}

export interface TruthFiles {
  readonly currentState: string;
  readonly particleLedger: string;
  readonly pendingHooks: string;
  readonly storyBible: string;
  readonly volumeOutline: string;
  readonly bookRules: string;
}

export interface BookStatusInfo {
  readonly bookId: string;
  readonly title: string;
  readonly genre: string;
  readonly platform: string;
  readonly status: string;
  readonly chaptersWritten: number;
  readonly totalWords: number;
  readonly nextChapter: number;
  readonly chapters: ReadonlyArray<ChapterMeta>;
}

export class PipelineRunner {
  private readonly state: StateManager;
  private readonly config: PipelineConfig;

  constructor(config: PipelineConfig) {
    this.config = config;
    this.state = new StateManager(config.projectRoot);
  }

  private agentCtx(bookId?: string): AgentContext {
    return {
      client: this.config.client,
      model: this.config.model,
      projectRoot: this.config.projectRoot,
      bookId,
    };
  }

  private modelFor(agentName: string): string {
    return this.config.modelOverrides?.[agentName] ?? this.config.model;
  }

  private agentCtxFor(agent: string, bookId?: string): AgentContext {
    return {
      client: this.config.client,
      model: this.modelFor(agent),
      projectRoot: this.config.projectRoot,
      bookId,
    };
  }

  private async loadGenreProfile(genre: string): Promise<{ profile: GenreProfile }> {
    const parsed = await readGenreProfile(this.config.projectRoot, genre);
    return { profile: parsed.profile };
  }

  // ---------------------------------------------------------------------------
  // Atomic operations (composable by OpenClaw or agent mode)
  // ---------------------------------------------------------------------------

  async runRadar(): Promise<RadarResult> {
    const radar = new RadarAgent(this.agentCtxFor("radar"), this.config.radarSources);
    return radar.scan();
  }

  async initBook(book: BookConfig): Promise<void> {
    const architect = new ArchitectAgent(this.agentCtxFor("architect", book.id));
    const bookDir = this.state.bookDir(book.id);

    await this.state.saveBookConfig(book.id, book);

    const { profile: gp } = await this.loadGenreProfile(book.genre);
    const foundation = await architect.generateFoundation(book, this.config.externalContext);
    await architect.writeFoundationFiles(bookDir, foundation, gp.numericalSystem);
    await this.state.saveChapterIndex(book.id, []);
  }

  /** Write a single draft chapter. Saves chapter file + truth files + index + snapshot. */
  async writeDraft(bookId: string, context?: string, wordCount?: number): Promise<DraftResult> {
    const releaseLock = await this.state.acquireBookLock(bookId);
    try {
      const book = await this.state.loadBookConfig(bookId);
      const bookDir = this.state.bookDir(bookId);
      const chapterNumber = await this.state.getNextChapterNumber(bookId);

      const { profile: gp } = await this.loadGenreProfile(book.genre);

      const writer = new WriterAgent(this.agentCtxFor("writer", bookId));
      const output = await writer.writeChapter({
        book,
        bookDir,
        chapterNumber,
        externalContext: context ?? this.config.externalContext,
        ...(wordCount ? { wordCountOverride: wordCount } : {}),
      });

      // Save chapter file
      const chaptersDir = join(bookDir, "chapters");
      const paddedNum = String(chapterNumber).padStart(4, "0");
      const sanitized = output.title.replace(/[/\\?%*:|"<>]/g, "").replace(/\s+/g, "_").slice(0, 50);
      const filename = `${paddedNum}_${sanitized}.md`;
      const filePath = join(chaptersDir, filename);

      await writeFile(filePath, `# 第${chapterNumber}章 ${output.title}\n\n${output.content}`, "utf-8");

      // Save truth files
      await writer.saveChapter(bookDir, output, gp.numericalSystem);
      await writer.saveNewTruthFiles(bookDir, output);

      // Update index
      const existingIndex = await this.state.loadChapterIndex(bookId);
      const now = new Date().toISOString();
      const newEntry: ChapterMeta = {
        number: chapterNumber,
        title: output.title,
        status: "drafted",
        wordCount: output.wordCount,
        createdAt: now,
        updatedAt: now,
        auditIssues: [],
      };
      await this.state.saveChapterIndex(bookId, [...existingIndex, newEntry]);

      // Snapshot
      await this.state.snapshotState(bookId, chapterNumber);

      await this.emitWebhook("chapter-complete", bookId, chapterNumber, {
        title: output.title,
        wordCount: output.wordCount,
      });

      return { chapterNumber, title: output.title, wordCount: output.wordCount, filePath };
    } finally {
      await releaseLock();
    }
  }

  /** Audit the latest (or specified) chapter. Read-only, no lock needed. */
  async auditDraft(bookId: string, chapterNumber?: number): Promise<AuditResult & { readonly chapterNumber: number }> {
    const book = await this.state.loadBookConfig(bookId);
    const bookDir = this.state.bookDir(bookId);
    const targetChapter = chapterNumber ?? (await this.state.getNextChapterNumber(bookId)) - 1;
    if (targetChapter < 1) {
      throw new Error(`No chapters to audit for "${bookId}"`);
    }

    const content = await this.readChapterContent(bookDir, targetChapter);
    const auditor = new ContinuityAuditor(this.agentCtxFor("auditor", bookId));
    const llmResult = await auditor.auditChapter(bookDir, content, targetChapter, book.genre);

    // Merge rule-based AI-tell detection
    const aiTells = analyzeAITells(content);
    // Merge sensitive word detection
    const sensitiveResult = analyzeSensitiveWords(content);
    const hasBlockedWords = sensitiveResult.found.some((f) => f.severity === "block");
    const mergedIssues: ReadonlyArray<AuditIssue> = [
      ...llmResult.issues,
      ...aiTells.issues,
      ...sensitiveResult.issues,
    ];
    const result: AuditResult = {
      passed: hasBlockedWords ? false : llmResult.passed,
      issues: mergedIssues,
      summary: llmResult.summary,
    };

    // Update index with audit result
    const index = await this.state.loadChapterIndex(bookId);
    const updated = index.map((ch) =>
      ch.number === targetChapter
        ? {
            ...ch,
            status: (result.passed ? "ready-for-review" : "audit-failed") as ChapterMeta["status"],
            updatedAt: new Date().toISOString(),
            auditIssues: result.issues.map((i) => `[${i.severity}] ${i.description}`),
          }
        : ch,
    );
    await this.state.saveChapterIndex(bookId, updated);

    await this.emitWebhook(
      result.passed ? "audit-passed" : "audit-failed",
      bookId,
      targetChapter,
      { summary: result.summary, issueCount: result.issues.length },
    );

    return { ...result, chapterNumber: targetChapter };
  }

  /** Revise the latest (or specified) chapter based on audit issues. */
  async reviseDraft(bookId: string, chapterNumber?: number, mode: ReviseMode = "rewrite"): Promise<ReviseResult> {
    const releaseLock = await this.state.acquireBookLock(bookId);
    try {
      const book = await this.state.loadBookConfig(bookId);
      const bookDir = this.state.bookDir(bookId);
      const targetChapter = chapterNumber ?? (await this.state.getNextChapterNumber(bookId)) - 1;
      if (targetChapter < 1) {
        throw new Error(`No chapters to revise for "${bookId}"`);
      }

      // Read the current audit issues from index
      const index = await this.state.loadChapterIndex(bookId);
      const chapterMeta = index.find((ch) => ch.number === targetChapter);
      if (!chapterMeta) {
        throw new Error(`Chapter ${targetChapter} not found in index`);
      }

      // Re-audit to get structured issues (index only stores strings)
      const content = await this.readChapterContent(bookDir, targetChapter);
      const auditor = new ContinuityAuditor(this.agentCtxFor("auditor", bookId));
      const auditResult = await auditor.auditChapter(bookDir, content, targetChapter, book.genre);

      if (auditResult.passed) {
        return { chapterNumber: targetChapter, wordCount: content.length, fixedIssues: ["No issues to fix"] };
      }

      const { profile: gp } = await this.loadGenreProfile(book.genre);

      const reviser = new ReviserAgent(this.agentCtxFor("reviser", bookId));
      const reviseOutput = await reviser.reviseChapter(
        bookDir, content, targetChapter, auditResult.issues, mode, book.genre,
      );

      if (reviseOutput.revisedContent.length === 0) {
        throw new Error("Reviser returned empty content");
      }

      // Save revised chapter file
      const chaptersDir = join(bookDir, "chapters");
      const files = await readdir(chaptersDir);
      const paddedNum = String(targetChapter).padStart(4, "0");
      const existingFile = files.find((f) => f.startsWith(paddedNum) && f.endsWith(".md"));
      if (existingFile) {
        await writeFile(
          join(chaptersDir, existingFile),
          `# 第${targetChapter}章 ${chapterMeta.title}\n\n${reviseOutput.revisedContent}`,
          "utf-8",
        );
      }

      // Update truth files
      const storyDir = join(bookDir, "story");
      if (reviseOutput.updatedState !== "(状态卡未更新)") {
        await writeFile(join(storyDir, "current_state.md"), reviseOutput.updatedState, "utf-8");
      }
      if (gp.numericalSystem && reviseOutput.updatedLedger && reviseOutput.updatedLedger !== "(账本未更新)") {
        await writeFile(join(storyDir, "particle_ledger.md"), reviseOutput.updatedLedger, "utf-8");
      }
      if (reviseOutput.updatedHooks !== "(伏笔池未更新)") {
        await writeFile(join(storyDir, "pending_hooks.md"), reviseOutput.updatedHooks, "utf-8");
      }

      // Update index
      const updatedIndex = index.map((ch) =>
        ch.number === targetChapter
          ? {
              ...ch,
              status: "ready-for-review" as ChapterMeta["status"],
              wordCount: reviseOutput.wordCount,
              updatedAt: new Date().toISOString(),
            }
          : ch,
      );
      await this.state.saveChapterIndex(bookId, updatedIndex);

      // Re-snapshot
      await this.state.snapshotState(bookId, targetChapter);

      await this.emitWebhook("revision-complete", bookId, targetChapter, {
        wordCount: reviseOutput.wordCount,
        fixedCount: reviseOutput.fixedIssues.length,
      });

      return {
        chapterNumber: targetChapter,
        wordCount: reviseOutput.wordCount,
        fixedIssues: reviseOutput.fixedIssues,
      };
    } finally {
      await releaseLock();
    }
  }

  /** Read all truth files for a book. */
  async readTruthFiles(bookId: string): Promise<TruthFiles> {
    const bookDir = this.state.bookDir(bookId);
    const storyDir = join(bookDir, "story");
    const readSafe = async (path: string): Promise<string> => {
      try {
        return await readFile(path, "utf-8");
      } catch {
        return "(文件不存在)";
      }
    };

    const [currentState, particleLedger, pendingHooks, storyBible, volumeOutline, bookRules] =
      await Promise.all([
        readSafe(join(storyDir, "current_state.md")),
        readSafe(join(storyDir, "particle_ledger.md")),
        readSafe(join(storyDir, "pending_hooks.md")),
        readSafe(join(storyDir, "story_bible.md")),
        readSafe(join(storyDir, "volume_outline.md")),
        readSafe(join(storyDir, "book_rules.md")),
      ]);

    return { currentState, particleLedger, pendingHooks, storyBible, volumeOutline, bookRules };
  }

  /** Get book status overview. */
  async getBookStatus(bookId: string): Promise<BookStatusInfo> {
    const book = await this.state.loadBookConfig(bookId);
    const chapters = await this.state.loadChapterIndex(bookId);
    const nextChapter = await this.state.getNextChapterNumber(bookId);
    const totalWords = chapters.reduce((sum, ch) => sum + ch.wordCount, 0);

    return {
      bookId,
      title: book.title,
      genre: book.genre,
      platform: book.platform,
      status: book.status,
      chaptersWritten: chapters.length,
      totalWords,
      nextChapter,
      chapters: [...chapters],
    };
  }

  // ---------------------------------------------------------------------------
  // Full pipeline (convenience — runs draft + audit + revise in one shot)
  // ---------------------------------------------------------------------------

  async writeNextChapter(bookId: string, wordCount?: number, temperatureOverride?: number): Promise<ChapterPipelineResult> {
    const releaseLock = await this.state.acquireBookLock(bookId);
    try {
      return await this._writeNextChapterLocked(bookId, wordCount, temperatureOverride);
    } finally {
      await releaseLock();
    }
  }

  private async _writeNextChapterLocked(bookId: string, wordCount?: number, temperatureOverride?: number): Promise<ChapterPipelineResult> {
    const book = await this.state.loadBookConfig(bookId);
    const bookDir = this.state.bookDir(bookId);
    const chapterNumber = await this.state.getNextChapterNumber(bookId);
    const { profile: gp } = await this.loadGenreProfile(book.genre);

    // 1. Write chapter
    const writer = new WriterAgent(this.agentCtxFor("writer", bookId));
    const output = await writer.writeChapter({
      book,
      bookDir,
      chapterNumber,
      externalContext: this.config.externalContext,
      ...(wordCount ? { wordCountOverride: wordCount } : {}),
      ...(temperatureOverride ? { temperatureOverride } : {}),
    });

    // 2. Audit chapter
    const auditor = new ContinuityAuditor(this.agentCtxFor("auditor", bookId));
    const llmAudit = await auditor.auditChapter(
      bookDir,
      output.content,
      chapterNumber,
      book.genre,
    );
    const aiTellsResult = analyzeAITells(output.content);
    const sensitiveWriteResult = analyzeSensitiveWords(output.content);
    const hasBlockedWriteWords = sensitiveWriteResult.found.some((f) => f.severity === "block");
    let auditResult: AuditResult = {
      passed: hasBlockedWriteWords ? false : llmAudit.passed,
      issues: [...llmAudit.issues, ...aiTellsResult.issues, ...sensitiveWriteResult.issues],
      summary: llmAudit.summary,
    };

    let finalContent = output.content;
    let finalWordCount = output.wordCount;
    let revised = false;

    // 3. If audit fails, try auto-revise once
    if (!auditResult.passed) {
      const criticalIssues = auditResult.issues.filter(
        (i) => i.severity === "critical",
      );
      if (criticalIssues.length > 0) {
        const reviser = new ReviserAgent(this.agentCtxFor("reviser", bookId));
        const reviseOutput = await reviser.reviseChapter(
          bookDir,
          output.content,
          chapterNumber,
          auditResult.issues,
          "rewrite",
          book.genre,
        );

        if (reviseOutput.revisedContent.length > 0) {
          finalContent = reviseOutput.revisedContent;
          finalWordCount = reviseOutput.wordCount;
          revised = true;

          // Re-audit the revised content
          const reAudit = await auditor.auditChapter(
            bookDir,
            finalContent,
            chapterNumber,
            book.genre,
          );
          const reAITells = analyzeAITells(finalContent);
          const reSensitive = analyzeSensitiveWords(finalContent);
          const reHasBlocked = reSensitive.found.some((f) => f.severity === "block");
          auditResult = {
            passed: reHasBlocked ? false : reAudit.passed,
            issues: [...reAudit.issues, ...reAITells.issues, ...reSensitive.issues],
            summary: reAudit.summary,
          };

          // Update state files from revision
          const storyDir = join(bookDir, "story");
          if (reviseOutput.updatedState !== "(状态卡未更新)") {
            await writeFile(join(storyDir, "current_state.md"), reviseOutput.updatedState, "utf-8");
          }
          if (gp.numericalSystem && reviseOutput.updatedLedger && reviseOutput.updatedLedger !== "(账本未更新)") {
            await writeFile(join(storyDir, "particle_ledger.md"), reviseOutput.updatedLedger, "utf-8");
          }
          if (reviseOutput.updatedHooks !== "(伏笔池未更新)") {
            await writeFile(join(storyDir, "pending_hooks.md"), reviseOutput.updatedHooks, "utf-8");
          }
        }
      }
    }

    // 4. Save chapter (original or revised)
    const chaptersDir = join(bookDir, "chapters");
    const paddedNum = String(chapterNumber).padStart(4, "0");
    const title = output.title;
    const filename = `${paddedNum}_${title.replace(/[/\\?%*:|"<>]/g, "").replace(/\s+/g, "_").slice(0, 50)}.md`;

    await writeFile(
      join(chaptersDir, filename),
      `# 第${chapterNumber}章 ${title}\n\n${finalContent}`,
      "utf-8",
    );

    // Save original state files if not revised
    if (!revised) {
      await writer.saveChapter(bookDir, output, gp.numericalSystem);
    }

    // Save new truth files (summaries, subplots, emotional arcs, character matrix)
    await writer.saveNewTruthFiles(bookDir, output);

    // 5. Update chapter index
    const existingIndex = await this.state.loadChapterIndex(bookId);
    const now = new Date().toISOString();
    const newEntry: ChapterMeta = {
      number: chapterNumber,
      title: output.title,
      status: auditResult.passed ? "ready-for-review" : "audit-failed",
      wordCount: finalWordCount,
      createdAt: now,
      updatedAt: now,
      auditIssues: auditResult.issues.map(
        (i) => `[${i.severity}] ${i.description}`,
      ),
    };
    await this.state.saveChapterIndex(bookId, [...existingIndex, newEntry]);

    // 5.5 Snapshot state for rollback support
    await this.state.snapshotState(bookId, chapterNumber);

    // 6. Send notification
    if (this.config.notifyChannels && this.config.notifyChannels.length > 0) {
      const statusEmoji = auditResult.passed ? "✅" : "⚠️";
      await dispatchNotification(this.config.notifyChannels, {
        title: `${statusEmoji} ${book.title} 第${chapterNumber}章`,
        body: [
          `**${output.title}** | ${finalWordCount}字`,
          revised ? "📝 已自动修正" : "",
          `审稿: ${auditResult.passed ? "通过" : "需人工审核"}`,
          ...auditResult.issues
            .filter((i) => i.severity !== "info")
            .map((i) => `- [${i.severity}] ${i.description}`),
        ]
          .filter(Boolean)
          .join("\n"),
      });
    }

    await this.emitWebhook("pipeline-complete", bookId, chapterNumber, {
      title: output.title,
      wordCount: finalWordCount,
      passed: auditResult.passed,
      revised,
    });

    return {
      chapterNumber,
      title: output.title,
      wordCount: finalWordCount,
      auditResult,
      revised,
      status: auditResult.passed ? "approved" : "needs-review",
    };
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private async emitWebhook(
    event: WebhookEvent,
    bookId: string,
    chapterNumber?: number,
    data?: Record<string, unknown>,
  ): Promise<void> {
    if (!this.config.notifyChannels || this.config.notifyChannels.length === 0) return;
    await dispatchWebhookEvent(this.config.notifyChannels, {
      event,
      bookId,
      chapterNumber,
      timestamp: new Date().toISOString(),
      data,
    });
  }

  private async readChapterContent(bookDir: string, chapterNumber: number): Promise<string> {
    const chaptersDir = join(bookDir, "chapters");
    const files = await readdir(chaptersDir);
    const paddedNum = String(chapterNumber).padStart(4, "0");
    const chapterFile = files.find((f) => f.startsWith(paddedNum) && f.endsWith(".md"));
    if (!chapterFile) {
      throw new Error(`Chapter ${chapterNumber} file not found in ${chaptersDir}`);
    }
    const raw = await readFile(join(chaptersDir, chapterFile), "utf-8");
    // Strip the title line
    const lines = raw.split("\n");
    const contentStart = lines.findIndex((l, i) => i > 0 && l.trim().length > 0);
    return contentStart >= 0 ? lines.slice(contentStart).join("\n") : raw;
  }
}
