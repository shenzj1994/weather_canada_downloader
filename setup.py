import setuptools

setuptools.setup(
    name='Canada Weather Data Downloader',
    version='0.1',
    packages=setuptools.find_packages(),
    url='',
    license='UNLICENSED',
    author='Zhongjie Shen',
    author_email='shenzj1994@gmail.com',
    description='A Python package to download weather data from Environment Canada.',
    install_requires=['requests', 'tqdm', 'pandas']
)
