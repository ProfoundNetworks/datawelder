import io
import os
import setuptools


def get_version():
    curr_dir = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(curr_dir, 'datawelder', 'version.py')) as fin:
        line = fin.readline().strip()
        parts = line.split(' ')
        assert len(parts) == 3
        assert parts[0] == '__version__'
        assert parts[1] == '='
        return parts[2].strip('\'"')


def read(fname):
    return io.open(os.path.join(os.path.dirname(__file__), fname), encoding='utf-8').read()


setuptools.setup(
    name='datawelder',
    version=get_version(),
    description='Joins large dataframes together',
    long_description=read('README.rst'),
    packages=setuptools.find_packages(),
    author='Michael Penkov',
    author_email='m@penkov.dev',
    maintainer='Michael Penkov',
    maintainer_email='m@penkov.dev',
    url='https://github.com/ProfoundNetworks/datawelder',
    download_url='http://pypi.python.org/pypi/datawelder',
    keywords='datawelder join dataframes',
    license='MIT',
    platforms='any',
    install_requires=['smart_open', 'pyyaml'],
    python_requires=">=3.6.*",
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Topic :: System :: Distributed Computing',
    ],
    extras_require={
        'test': ['pytest', 'pytest-cov'],
    }
)
