def classify_signal(mode: str, stage1_score: float, fib_ok: bool, rr: float, divergence_strength: float):
    warnings = []
    rejected_by = []

    if mode == "main":
        if stage1_score < 2.2:
            rejected_by.append("stage1_score_low")
        if not fib_ok:
            rejected_by.append("fib_zone_fail")
        if rr < 1.3:
            rejected_by.append("rr_too_low")
        if divergence_strength < 0.9:
            rejected_by.append("divergence_weak")
        passed = not rejected_by
        return passed, ("ready" if passed else "watch"), warnings, rejected_by

    if stage1_score < 1.2:
        rejected_by.append("stage1_score_low")
    if rr < 0.9:
        rejected_by.append("rr_too_low")
    passed = len(rejected_by) == 0
    if not fib_ok:
        warnings.append("fib_zone_not_confirmed")
    if divergence_strength < 0.7:
        warnings.append("divergence_weak")
    return passed, "watch", warnings, rejected_by
