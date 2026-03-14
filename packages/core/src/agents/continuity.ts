import { BaseAgent } from "./base.js";
import type { GenreProfile } from "../models/genre-profile.js";
import type { BookRules } from "../models/book-rules.js";
import { readGenreProfile, readBookRules } from "./rules-reader.js";
import { readFile } from "node:fs/promises";
import { join } from "node:path";

export interface AuditResult {
  readonly passed: boolean;
  readonly issues: ReadonlyArray<AuditIssue>;
  readonly summary: string;
}

export interface AuditIssue {
  readonly severity: "critical" | "warning" | "info";
  readonly category: string;
  readonly description: string;
  readonly suggestion: string;
}

// Dimension ID → name mapping
const DIMENSION_MAP: Record<number, string> = {
  1: "OOC检查",
  2: "时间线检查",
  3: "设定冲突",
  4: "战力崩坏",
  5: "数值检查",
  6: "伏笔检查",
  7: "节奏检查",
  8: "文风检查",
  9: "信息越界",
  10: "词汇疲劳",
  11: "利益链断裂",
  12: "年代考据",
  13: "配角降智",
  14: "配角工具人化",
  15: "爽点虚化",
  16: "台词失真",
  17: "流水账",
  18: "知识库污染",
  19: "视角一致性",
  20: "段落等长",
  21: "套话密度",
  22: "公式化转折",
  23: "列表式结构",
  24: "支线停滞",
  25: "弧线平坦",
  26: "节奏单调",
  27: "敏感词检查",
};

function buildDimensionList(
  gp: GenreProfile,
  bookRules: BookRules | null,
): ReadonlyArray<{ readonly id: number; readonly name: string; readonly note: string }> {
  const activeIds = new Set(gp.auditDimensions);

  // Add book-level additional dimensions
  if (bookRules?.additionalAuditDimensions) {
    for (const d of bookRules.additionalAuditDimensions) {
      if (typeof d === "number") activeIds.add(d);
    }
  }

  // Conditional overrides
  if (gp.eraResearch || bookRules?.eraConstraints?.enabled) {
    activeIds.add(12);
  }

  const dims: Array<{ id: number; name: string; note: string }> = [];

  for (const id of [...activeIds].sort((a, b) => a - b)) {
    const name = DIMENSION_MAP[id];
    if (!name) continue;

    let note = "";
    if (id === 10 && gp.fatigueWords.length > 0) {
      const words = bookRules?.fatigueWordsOverride && bookRules.fatigueWordsOverride.length > 0
        ? bookRules.fatigueWordsOverride
        : gp.fatigueWords;
      note = `高疲劳词：${words.join("、")}。同时检查AI标记词（仿佛/不禁/宛如/竟然/忽然/猛地）密度，每3000字超过1次即warning`;
    }
    if (id === 15 && gp.satisfactionTypes.length > 0) {
      note = `爽点类型：${gp.satisfactionTypes.join("、")}`;
    }
    if (id === 12 && bookRules?.eraConstraints) {
      const era = bookRules.eraConstraints;
      const parts = [era.period, era.region].filter(Boolean);
      if (parts.length > 0) note = `年代：${parts.join("，")}`;
    }
    if (id === 19) {
      note = "检查视角切换是否有过渡、是否与设定视角一致";
    }
    if (id === 24) {
      note = "检查支线剧情是否停滞超过5章未推进";
    }
    if (id === 25) {
      note = "检查主要角色情感弧线是否平坦（连续3章无情绪变化）";
    }
    if (id === 26) {
      note = "检查章节类型节奏：连续≥3同类型章→warning，≥5章无高潮/回收→warning";
    }

    dims.push({ id, name, note });
  }

  return dims;
}

export class ContinuityAuditor extends BaseAgent {
  get name(): string {
    return "continuity-auditor";
  }

  async auditChapter(
    bookDir: string,
    chapterContent: string,
    chapterNumber: number,
    genre?: string,
  ): Promise<AuditResult> {
    const [currentState, ledger, hooks, styleGuideRaw, subplotBoard, emotionalArcs, characterMatrix, chapterSummaries] =
      await Promise.all([
        this.readFileSafe(join(bookDir, "story/current_state.md")),
        this.readFileSafe(join(bookDir, "story/particle_ledger.md")),
        this.readFileSafe(join(bookDir, "story/pending_hooks.md")),
        this.readFileSafe(join(bookDir, "story/style_guide.md")),
        this.readFileSafe(join(bookDir, "story/subplot_board.md")),
        this.readFileSafe(join(bookDir, "story/emotional_arcs.md")),
        this.readFileSafe(join(bookDir, "story/character_matrix.md")),
        this.readFileSafe(join(bookDir, "story/chapter_summaries.md")),
      ]);

    // Load genre profile and book rules
    const genreId = genre ?? "other";
    const { profile: gp } = await readGenreProfile(this.ctx.projectRoot, genreId);
    const parsedRules = await readBookRules(bookDir);
    const bookRules = parsedRules?.rules ?? null;

    // Fallback: use book_rules body when style_guide.md doesn't exist
    const styleGuide = styleGuideRaw !== "(文件不存在)"
      ? styleGuideRaw
      : (parsedRules?.body ?? "(无文风指南)");

    const dimensions = buildDimensionList(gp, bookRules);
    const dimList = dimensions
      .map((d) => `${d.id}. ${d.name}${d.note ? `（${d.note}）` : ""}`)
      .join("\n");

    const protagonistBlock = bookRules?.protagonist
      ? `\n主角人设锁定：${bookRules.protagonist.name}，${bookRules.protagonist.personalityLock.join("、")}，行为约束：${bookRules.protagonist.behavioralConstraints.join("、")}`
      : "";

    const systemPrompt = `你是一位严格的${gp.name}网络小说审稿编辑。你的任务是对章节进行连续性、一致性和质量审查。${protagonistBlock}

审查维度：
${dimList}

输出格式必须为 JSON：
{
  "passed": true/false,
  "issues": [
    {
      "severity": "critical|warning|info",
      "category": "审查维度名称",
      "description": "具体问题描述",
      "suggestion": "修改建议"
    }
  ],
  "summary": "一句话总结审查结论"
}

只有当存在 critical 级别问题时，passed 才为 false。`;

    const ledgerBlock = gp.numericalSystem
      ? `\n## 资源账本\n${ledger}`
      : "";

    const subplotBlock = subplotBoard !== "(文件不存在)"
      ? `\n## 支线进度板\n${subplotBoard}\n`
      : "";
    const emotionalBlock = emotionalArcs !== "(文件不存在)"
      ? `\n## 情感弧线\n${emotionalArcs}\n`
      : "";
    const matrixBlock = characterMatrix !== "(文件不存在)"
      ? `\n## 角色交互矩阵\n${characterMatrix}\n`
      : "";
    const summariesBlock = chapterSummaries !== "(文件不存在)"
      ? `\n## 章节摘要（用于节奏检查）\n${chapterSummaries}\n`
      : "";

    const userPrompt = `请审查第${chapterNumber}章。

## 当前状态卡
${currentState}
${ledgerBlock}
## 伏笔池
${hooks}
${subplotBlock}${emotionalBlock}${matrixBlock}${summariesBlock}
## 文风指南
${styleGuide}

## 待审章节内容
${chapterContent}`;

    const response = await this.chat(
      [
        { role: "system", content: systemPrompt },
        { role: "user", content: userPrompt },
      ],
      { temperature: 0.3, maxTokens: 4096 },
    );

    return this.parseAuditResult(response.content);
  }

  private parseAuditResult(content: string): AuditResult {
    const jsonMatch = content.match(/\{[\s\S]*\}/);
    if (!jsonMatch) {
      return {
        passed: false,
        issues: [
          {
            severity: "critical",
            category: "系统错误",
            description: "审稿输出格式异常，无法解析",
            suggestion: "重新运行审稿",
          },
        ],
        summary: "审稿输出解析失败",
      };
    }

    try {
      const parsed = JSON.parse(jsonMatch[0]);
      return {
        passed: Boolean(parsed.passed),
        issues: Array.isArray(parsed.issues) ? parsed.issues : [],
        summary: String(parsed.summary ?? ""),
      };
    } catch {
      return {
        passed: false,
        issues: [
          {
            severity: "critical",
            category: "系统错误",
            description: "审稿 JSON 解析失败",
            suggestion: "重新运行审稿",
          },
        ],
        summary: "审稿 JSON 解析失败",
      };
    }
  }

  private async readFileSafe(path: string): Promise<string> {
    try {
      return await readFile(path, "utf-8");
    } catch {
      return "(文件不存在)";
    }
  }
}
