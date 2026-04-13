from __future__ import annotations


def grade_from_score(score: float) -> str:
    if score >= 90:
        return 'A'
    if score >= 78:
        return 'B'
    if score >= 66:
        return 'C'
    return 'D'
