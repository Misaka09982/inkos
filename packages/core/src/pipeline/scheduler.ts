import { PipelineRunner } from "./runner.js";
import type { PipelineConfig } from "./runner.js";
import { StateManager } from "../state/manager.js";
import type { BookConfig } from "../models/book.js";
import type { QualityGates, DetectionConfig } from "../models/project.js";
import { dispatchWebhookEvent } from "../notify/dispatcher.js";
import { detectChapter, detectAndRewrite } from "./detection-runner.js";

export interface SchedulerConfig extends PipelineConfig {
  readonly radarCron: string;
  readonly writeCron: string;
  readonly auditCron: string;
  readonly maxConcurrentBooks: number;
  readonly qualityGates?: QualityGates;
  readonly detection?: DetectionConfig;
  readonly onChapterComplete?: (bookId: string, chapter: number, status: string) => void;
  readonly onError?: (bookId: string, error: Error) => void;
  readonly onPause?: (bookId: string, reason: string) => void;
}

interface ScheduledTask {
  readonly name: string;
  readonly intervalMs: number;
  timer?: ReturnType<typeof setInterval>;
}

export class Scheduler {
  private readonly pipeline: PipelineRunner;
  private readonly state: StateManager;
  private readonly config: SchedulerConfig;
  private tasks: ScheduledTask[] = [];
  private running = false;

  // Quality gate tracking (per book)
  private consecutiveFailures = new Map<string, number>();
  private pausedBooks = new Set<string>();
  // Failure clustering: bookId → (dimension → count)
  private failureDimensions = new Map<string, Map<string, number>>();

  constructor(config: SchedulerConfig) {
    this.config = config;
    this.pipeline = new PipelineRunner(config);
    this.state = new StateManager(config.projectRoot);
  }

  async start(): Promise<void> {
    if (this.running) return;
    this.running = true;

    // Run write cycle immediately on start, then schedule
    await this.runWriteCycle();

    // Schedule recurring write cycle (default: every 2 hours)
    const writeCycleMs = this.cronToMs(this.config.writeCron);
    const writeTask: ScheduledTask = {
      name: "write-cycle",
      intervalMs: writeCycleMs,
    };
    writeTask.timer = setInterval(() => {
      this.runWriteCycle().catch((e) => {
        this.config.onError?.("scheduler", e as Error);
      });
    }, writeCycleMs);
    this.tasks.push(writeTask);

    // Schedule radar scan (default: daily)
    const radarMs = this.cronToMs(this.config.radarCron);
    const radarTask: ScheduledTask = {
      name: "radar-scan",
      intervalMs: radarMs,
    };
    radarTask.timer = setInterval(() => {
      this.runRadarScan().catch((e) => {
        this.config.onError?.("radar", e as Error);
      });
    }, radarMs);
    this.tasks.push(radarTask);
  }

  stop(): void {
    this.running = false;
    for (const task of this.tasks) {
      if (task.timer) clearInterval(task.timer);
    }
    this.tasks = [];
  }

  get isRunning(): boolean {
    return this.running;
  }

  /** Resume a paused book. */
  resumeBook(bookId: string): void {
    this.pausedBooks.delete(bookId);
    this.consecutiveFailures.delete(bookId);
    this.failureDimensions.delete(bookId);
  }

  /** Check if a book is paused. */
  isBookPaused(bookId: string): boolean {
    return this.pausedBooks.has(bookId);
  }

  private get gates(): QualityGates {
    return this.config.qualityGates ?? {
      maxAuditRetries: 2,
      pauseAfterConsecutiveFailures: 3,
      retryTemperatureStep: 0.1,
    };
  }

  private async runWriteCycle(): Promise<void> {
    const bookIds = await this.state.listBooks();

    const activeBooks: Array<{ id: string; config: BookConfig }> = [];
    for (const id of bookIds) {
      if (this.pausedBooks.has(id)) continue; // Skip paused books
      const config = await this.state.loadBookConfig(id);
      if (config.status === "active" || config.status === "outlining") {
        activeBooks.push({ id, config });
      }
    }

    const booksToWrite = activeBooks.slice(0, this.config.maxConcurrentBooks);

    for (const book of booksToWrite) {
      try {
        // Compute temperature override: base 0.7 + failures * step
        const failures = this.consecutiveFailures.get(book.id) ?? 0;
        const tempOverride = failures > 0
          ? Math.min(1.2, 0.7 + failures * this.gates.retryTemperatureStep)
          : undefined;

        const result = await this.pipeline.writeNextChapter(book.id, undefined, tempOverride);

        if (result.status === "approved") {
          // Reset failure counter on success
          this.consecutiveFailures.delete(book.id);

          // Feature #6: Auto-detection loop after successful audit
          if (this.config.detection?.enabled) {
            try {
              const bookDir = this.state.bookDir(book.id);
              const chapterContent = await this.readChapterContent(bookDir, result.chapterNumber);
              const detResult = await detectChapter(
                this.config.detection,
                chapterContent,
                result.chapterNumber,
              );
              if (!detResult.passed && this.config.detection.autoRewrite) {
                await detectAndRewrite(
                  this.config.detection,
                  { client: this.config.client, model: this.config.model, projectRoot: this.config.projectRoot },
                  bookDir,
                  chapterContent,
                  result.chapterNumber,
                  book.config.genre,
                );
              }
            } catch (e) {
              this.config.onError?.(book.id, e as Error);
            }
          }
        } else {
          // Audit failed — apply quality gates
          const issueCategories = result.auditResult.issues.map((i) => i.category);
          await this.handleAuditFailure(book.id, result.chapterNumber, issueCategories);
        }

        this.config.onChapterComplete?.(
          book.id,
          result.chapterNumber,
          result.status,
        );
      } catch (e) {
        this.config.onError?.(book.id, e as Error);
        await this.handleAuditFailure(book.id, 0);
      }
    }
  }

  private async handleAuditFailure(
    bookId: string,
    chapterNumber: number,
    issueCategories: ReadonlyArray<string> = [],
  ): Promise<void> {
    const failures = (this.consecutiveFailures.get(bookId) ?? 0) + 1;
    this.consecutiveFailures.set(bookId, failures);

    // Track failure dimensions for clustering
    if (issueCategories.length > 0) {
      const dimMap = this.failureDimensions.get(bookId) ?? new Map<string, number>();
      for (const cat of issueCategories) {
        dimMap.set(cat, (dimMap.get(cat) ?? 0) + 1);
      }
      this.failureDimensions.set(bookId, dimMap);

      // Check for dimension clustering (any dimension with >=3 failures)
      for (const [dimension, count] of dimMap) {
        if (count >= 3) {
          await this.emitDiagnosticAlert(bookId, chapterNumber, dimension, count);
        }
      }
    }

    const gates = this.gates;

    // Check if we should retry with higher temperature
    if (failures <= gates.maxAuditRetries) {
      process.stderr.write(
        `[scheduler] ${bookId} audit failed (${failures}/${gates.maxAuditRetries}), retrying with higher temperature\n`,
      );
      // The retry will happen in the next write cycle with adjusted temperature
      // (We don't retry immediately to avoid tight loops)
      return;
    }

    // Check if we should pause
    if (failures >= gates.pauseAfterConsecutiveFailures) {
      this.pausedBooks.add(bookId);
      const reason = `${failures} consecutive audit failures (threshold: ${gates.pauseAfterConsecutiveFailures})`;
      process.stderr.write(`[scheduler] ${bookId} PAUSED: ${reason}\n`);
      this.config.onPause?.(bookId, reason);

      // Emit webhook event
      if (this.config.notifyChannels && this.config.notifyChannels.length > 0) {
        await dispatchWebhookEvent(this.config.notifyChannels, {
          event: "pipeline-error",
          bookId,
          chapterNumber: chapterNumber > 0 ? chapterNumber : undefined,
          timestamp: new Date().toISOString(),
          data: { reason, consecutiveFailures: failures },
        });
      }
    }
  }

  private async runRadarScan(): Promise<void> {
    try {
      await this.pipeline.runRadar();
    } catch (e) {
      this.config.onError?.("radar", e as Error);
    }
  }

  private async emitDiagnosticAlert(
    bookId: string,
    chapterNumber: number,
    dimension: string,
    count: number,
  ): Promise<void> {
    process.stderr.write(
      `[scheduler] DIAGNOSTIC: ${bookId} has ${count} failures in dimension "${dimension}"\n`,
    );

    if (this.config.notifyChannels && this.config.notifyChannels.length > 0) {
      await dispatchWebhookEvent(this.config.notifyChannels, {
        event: "diagnostic-alert",
        bookId,
        chapterNumber: chapterNumber > 0 ? chapterNumber : undefined,
        timestamp: new Date().toISOString(),
        data: { dimension, failureCount: count },
      });
    }
  }

  private async readChapterContent(bookDir: string, chapterNumber: number): Promise<string> {
    const { readFile, readdir } = await import("node:fs/promises");
    const { join } = await import("node:path");
    const chaptersDir = join(bookDir, "chapters");
    const files = await readdir(chaptersDir);
    const paddedNum = String(chapterNumber).padStart(4, "0");
    const chapterFile = files.find((f) => f.startsWith(paddedNum) && f.endsWith(".md"));
    if (!chapterFile) {
      throw new Error(`Chapter ${chapterNumber} file not found in ${chaptersDir}`);
    }
    const raw = await readFile(join(chaptersDir, chapterFile), "utf-8");
    const lines = raw.split("\n");
    const contentStart = lines.findIndex((l, i) => i > 0 && l.trim().length > 0);
    return contentStart >= 0 ? lines.slice(contentStart).join("\n") : raw;
  }

  private cronToMs(cron: string): number {
    // Simple cron-to-interval mapping for common patterns
    // "0 9 * * *" = daily = 24h
    // "0 14 * * *" = daily = 24h
    // "0 */2 * * *" = every 2h
    const parts = cron.split(" ");
    if (parts.length >= 5) {
      const hour = parts[1]!;
      if (hour.startsWith("*/")) {
        const interval = parseInt(hour.slice(2), 10);
        return interval * 60 * 60 * 1000;
      }
      // Default: treat as daily
      return 24 * 60 * 60 * 1000;
    }
    return 24 * 60 * 60 * 1000;
  }
}
