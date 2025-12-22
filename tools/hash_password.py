from __future__ import annotations

from getpass import getpass

from werkzeug.security import generate_password_hash


def main() -> int:
    pw1 = getpass("서버에 설정할 웹 비밀번호를 입력하세요: ")
    pw2 = getpass("한 번 더 입력하세요: ")
    if not pw1 or pw1 != pw2:
        print("비밀번호가 비어있거나 서로 다릅니다.")
        return 1
    print(generate_password_hash(pw1))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

