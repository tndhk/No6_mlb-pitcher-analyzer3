# setup.py
from setuptools import setup, find_packages

setup(
    name="mlb_pitcher_dashboard",
    version="0.1.0",
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        "streamlit>=1.25.0",
        "pybaseball>=2.2.5",
        "pandas>=2.0.3",
        "numpy>=1.24.4",
        "plotly>=5.15.0",
    ],
    entry_points={
        "console_scripts": [
            "mlb-dashboard=src.app:main",
        ],
    },
    python_requires=">=3.8",
    author="Your Name",
    author_email="your.email@example.com",
    description="MLB Pitcher Performance Dashboard",
    keywords="baseball, mlb, data visualization, streamlit, dashboard",
    project_urls={
        "Source Code": "https://github.com/yourusername/mlb-pitcher-dashboard",
    },
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Sports Fans",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
    ],
)