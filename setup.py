from setuptools import find_packages, setup

if __name__ == "__main__":
    setup(
        name="buck-it",
        package_dir={"": "src"},
        install_requires=[
            "kafkahelpers >= 0.3.1",
            "aiobotocore",
            "aiokafka",
            "prometheus-client",
            "prometheus_async",
        ],
        extras_require={"tests": ["coverage", "flake8", "pytest", "pytest-asyncio"]},
        include_package_data=True,
    )
