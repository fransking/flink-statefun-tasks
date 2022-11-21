import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

install_requires = [
    'apache-flink-statefun>=3.2.0',
    'kafka-python'
]

setuptools.setup(
    name="statefun-tasks",
    version="0.9.88",
    author="Frans King & Luke Ashworth",
    author_email="frans.king@sbbsystems.com",
    description="Tasks API for Stateful Functions on Flink",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://fransking.github.io/flink-statefun-tasks",
    packages=['statefun_tasks'] + [f'statefun_tasks.{package}' for package in setuptools.find_packages('statefun_tasks')],
    license='https://www.apache.org/licenses/LICENSE-2.0',
    license_files=["LICENSE"],
    install_requires=install_requires,
    python_requires='>=3.8',
    classifiers=[
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 3.8']
)
