import sys
import datawelder.partition


def main():
    path = sys.argv[1]
    try:
        keyindex = int(sys.argv[2])
    except Exception:
        keyindex = 0
    datawelder.partition.sort_partition(path, keyindex)


if __name__ == '__main__':
    main()
