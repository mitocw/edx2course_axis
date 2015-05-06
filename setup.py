from setuptools import setup

setup(
    name='edx2course_axis',
    version='0.1.0',
    packages=['edx2course_axis'],
    scripts=['bin/edx2course_axis'],
    author='Isaac Chuang, Shawn Milochik',
    author_email='ichuang@mit.edu, milochik@mit.edu',
    description='Converts edX courses into text, CSV, and xbundle formats.',
    url='https://github.com/mitodl/edx2course_axis',
    install_requires=['lxml', 'BeautifulSoup', 'xbundle'],
)
