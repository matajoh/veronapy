from setuptools import Extension, setup

setup(
    ext_modules=[
        Extension(
            name="veronapy",
            sources=["src/veronapy.c"],
        ),
    ]
)
