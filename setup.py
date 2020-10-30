import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

install_requires = ['apache-flink-statefun>=2.2.0']

setuptools.setup(
    name="statefun-tasks", # Replace with your own username
    version="0.4.4",
    author="Frans King",
    author_email="frans.king@sbbsystems.com",
    description="Tasks API for Stateful Functions on Flink",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/fransking/flink-statefun-tasks",
    packages=['statefun_tasks', 'statefun_tasks.client'],
    license='https://www.apache.org/licenses/LICENSE-2.0',
    license_files=["LICENSE"],
    install_requires=install_requires,
    python_requires='>=3.5',
    classifiers=[
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7']
)