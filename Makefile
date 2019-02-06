all:
	python setup.py build && python setup.py install
pack:
	python3 setup.py sdist bdist_wheel && python3 -m twine upload dist/*

install: all

unittest: all
	pytest -s -v -q ./pyslait/test_client.py
