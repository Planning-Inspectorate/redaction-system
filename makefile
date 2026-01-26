run:
	azurite > /tmp/azurite.log 2>&1 &
	cd redactor && func start

trigger:
	python3 scripts/trigger_redaction.py