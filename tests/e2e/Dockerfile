# The end-to-end runner.
#
# model.py imports nothing from Qt, so the suite it drives needs no display,
# no X libraries and no PyQt6 — just boto3. That is what keeps this image
# small and the run hermetic.
FROM python:3.12-slim

# Defaults to requirements.txt: an e2e run that disagrees with the app about
# its own SDK version is testing something else.
#
# Overridable because the version is itself a dimension worth testing. The
# .deb depends on the distribution's python3-botocore, which is years behind
# this pin, and parameters S3 gained since then (IfNoneMatch, IfMatch on
# PutObject, ChecksumType) are refused client-side by boto3 before a request
# is ever sent. Only a run on an old SDK exercises the code that stands down
# from them; see the runner-old service in docker-compose.yml.
ARG BOTO3_VERSION=1.42.44
RUN pip install --no-cache-dir boto3==${BOTO3_VERSION}

WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1

# The tree is mounted read-only by compose; nothing is copied in, so a run
# always tests the working tree rather than a stale layer.
CMD ["python", "-m", "unittest", "discover", "-s", "tests/e2e", "-t", ".", "-v"]
