import sys

from structures import Database

def main(file: str):
    db = Database.parse_file(filename=file)
    print(db)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python cli.py <filename>")
        sys.exit(1)
    main(sys.argv[1])