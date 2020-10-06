"""doctest-like functionality for README.rst"""
import shlex
import subprocess


def g():
    command = None
    expected_output = []
    with open('README.rst') as fin:
        while True:
            line = fin.readline()
            if not line:
                break
            elif line.startswith('    $'):
                if command:
                    yield command, '\n'.join(expected_output)

                command = line.replace('    $', '')
                expected_output = []
            elif line.startswith('    ') and command:
                expected_output.append(line.strip())

        if command:
            yield command, '\n'.join(expected_output)


for (command, expected_output) in g():
    print(command.strip())

    stdout = subprocess.check_output(shlex.split(command)).strip().decode('utf-8')
    expected_output = expected_output.strip()
    if stdout == expected_output:
        print('OK')
    else:
        print('>>>>')
        print(expected_output)
        print('====')
        print(stdout)
        print('<<<<')
        print()
