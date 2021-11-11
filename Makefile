init:
	pip3 install pipenv

pipenv-venv: init
	pipenv --three

pipenv-activate: pipenv-venv 
	pipenv shell

pipenv-run: pipenv-activate
	pipenv install

