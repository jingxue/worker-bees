import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="worker-bees-jingxue", # Replace with your own username
    version="0.0.1",
    author="Jing Xue",
    author_email="jingxue@digizenstudio.com",
    description="A job chunk processing/aggregation engine.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)