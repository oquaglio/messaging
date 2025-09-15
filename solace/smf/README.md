# solace smf

## Setup

Set ENV:
```SH
export SOLACE_HOST=tcp://your-broker-host:55555
export SOLACE_VPN=your-vpn
export SOLACE_USERNAME=your-username
export SOLACE_PASSWORD=your-password
```

E.g.:
```SH
export SOLACE_HOST=tcp://localhost:55555
export SOLACE_VPN=default
export SOLACE_USERNAME=admin
export SOLACE_PASSWORD=admin
```

Enable virt env (I am using pyenv/virtualenv here):

```SH
pyenv_ver=3.13.5
```

Install if needed:
```SH
pyenv deactivate
pyenv install $pyenv_ver
```

Create/activate virtenv:
```SH
pyenv_env_name=solace-testing
pyenv virtualenv $pyenv_ver $pyenv_env_name;
pyenv activate $pyenv_env_name
pip install -r requirements.txt
pip install pipdeptree
pipdeptree
```

Cleanup virtenv:
```SH
pyenv deactivate; yes | pyenv virtualenv-delete $pyenv_env_name
```

## Run

```SH
python solace_loadtest_publisher.py.
```

Run JSON load tester with Desired Payload Size:
```SH
For 1KB: python solace_loadtest_publisher_json.py --size 1
For 10KB: python solace_loadtest_publisher_json.py --size 10
For 100KB: python solace_loadtest_publisher_json.py --size 100
For 1000KB: python solace_loadtest_publisher_json.py --size 1000
```
