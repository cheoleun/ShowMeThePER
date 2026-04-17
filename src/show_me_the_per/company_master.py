from __future__ import annotations

import csv
import json
from dataclasses import asdict
from pathlib import Path

from .models import MatchResult


def build_company_master_payload(result: MatchResult) -> dict[str, object]:
    return {
        "summary": build_match_summary(result),
        "matched": [asdict(company) for company in result.matched],
        "unmatched_listings": [
            asdict(listing) for listing in result.unmatched_listings
        ],
        "ambiguous_matches": [
            {
                "listing": asdict(match.listing),
                "candidates": [asdict(candidate) for candidate in match.candidates],
            }
            for match in result.ambiguous_matches
        ],
    }


def build_match_summary(result: MatchResult) -> dict[str, int | float]:
    matched_count = len(result.matched)
    total = result.total_listings
    match_rate = matched_count / total if total else 0.0

    return {
        "total_listings": total,
        "matched": matched_count,
        "unmatched_listings": len(result.unmatched_listings),
        "ambiguous_matches": len(result.ambiguous_matches),
        "match_rate": round(match_rate, 6),
    }


def write_company_master_outputs(
    result: MatchResult,
    output_json: Path,
    matched_csv: Path | None = None,
    unmatched_csv: Path | None = None,
    ambiguous_json: Path | None = None,
    report_markdown: Path | None = None,
) -> None:
    write_json(output_json, build_company_master_payload(result))

    if matched_csv is not None:
        write_csv(matched_csv, [asdict(company) for company in result.matched])
    if unmatched_csv is not None:
        write_csv(
            unmatched_csv,
            [asdict(listing) for listing in result.unmatched_listings],
        )
    if ambiguous_json is not None:
        write_json(
            ambiguous_json,
            [
                {
                    "listing": asdict(match.listing),
                    "candidates": [asdict(candidate) for candidate in match.candidates],
                }
                for match in result.ambiguous_matches
            ],
        )
    if report_markdown is not None:
        write_match_report(report_markdown, result)


def write_match_report(path: Path, result: MatchResult) -> None:
    summary = build_match_summary(result)
    lines = [
        "# 기업 마스터 매칭 리포트",
        "",
        f"- 전체 상장 종목: {summary['total_listings']}",
        f"- 매칭 성공: {summary['matched']}",
        f"- 미매칭: {summary['unmatched_listings']}",
        f"- 중복 후보: {summary['ambiguous_matches']}",
        f"- 매칭률: {summary['match_rate']:.2%}",
        "",
        "## 미매칭 종목",
        "",
    ]

    if result.unmatched_listings:
        lines.extend(
            f"- {listing.market} {listing.short_code} {listing.item_name}"
            for listing in result.unmatched_listings
        )
    else:
        lines.append("- 없음")

    lines.extend(["", "## 중복 후보", ""])

    if result.ambiguous_matches:
        for match in result.ambiguous_matches:
            candidate_names = ", ".join(
                f"{candidate.corp_code} {candidate.corp_name}"
                for candidate in match.candidates
            )
            lines.append(
                f"- {match.listing.short_code} {match.listing.item_name}: "
                f"{candidate_names}"
            )
    else:
        lines.append("- 없음")

    write_text(path, "\n".join(lines) + "\n")


def write_json(path: Path, payload: object) -> None:
    write_text(path, json.dumps(payload, ensure_ascii=False, indent=2) + "\n")


def write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = sorted({field for row in rows for field in row.keys()})
    with path.open("w", encoding="utf-8", newline="") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
