def replace_within_parentheses(text: str, delimiter: str = ",", replacement: str = "§") -> str:
    depth = 0
    text_list = list(text)

    for i in range(len(text_list)):
        if text_list[i] == "(":
            depth += 1
        elif text_list[i] == ")":
            depth -= 1

        if text_list[i] == delimiter and depth > 0:
            text_list[i] = replacement

    return "".join(text_list)


def strip_outer_parentheses(s: str) -> list[str]:
    return [f.replace("§", ",") for f in replace_within_parentheses(s).split(", ")]
