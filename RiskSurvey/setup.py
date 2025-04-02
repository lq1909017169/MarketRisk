try:
    from setuptools import setup
except:
    from distutils.core import setup

setup(
    name='Simple',
    version=0.02,
    author='stay',
    install_requires=['pandas'],
    packages=['Simple', 'Simple.core', 'Simple.handle', 'Simple.utils']
)
