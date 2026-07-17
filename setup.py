from setuptools import setup

setup(
    name="relayq",
    version="0.1.0",
    description="Distributed job queue with Redis Streams transport",
    python_requires=">=3.11",
    license="MIT",
    install_requires=[
        "redis[hiredis]>=5.0",
        "prometheus-client>=0.19",
        "fastapi>=0.109",
        "uvicorn[standard]>=0.27",
        "pydantic>=2.5",
    ],
    extras_require={
        "dev": [
            "pytest>=8.0",
            "pytest-asyncio>=0.23",
            "pytest-cov>=4.1",
            "hypothesis>=6.92",
            "httpx>=0.26",
        ],
    },
)
