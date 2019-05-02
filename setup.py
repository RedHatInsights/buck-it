from setuptools import find_packages, setup

if __name__ == "__main__":
    setup(
        name="buck-it",
        packages=find_packages(where="src"),
        package_dir={"": "src"},
        install_requires=[
            "kafkahelpers >= 0.3.1",
            "aiobotocore",
            "aiokafka",
            "prometheus-client",
            "prometheus_async",
        ],
        entry_points={"console_scripts": "buckit = buckit.app:main"},
        extras_require={"tests": ["coverage", "flake8", "pytest", "pytest-asyncio"]},
        include_package_data=True,
    )
