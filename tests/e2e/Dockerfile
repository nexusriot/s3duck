# The end-to-end runner.
#
# model.py imports nothing from Qt, so the suite it drives needs no display,
# no X libraries and no PyQt6 — just boto3. That is what keeps this image
# small and the run hermetic.
FROM python:3.12-slim

# Pinned to match requirements.txt: an e2e run that disagrees with the app
# about its own SDK version is testing something else.
RUN pip install --no-cache-dir boto3==1.42.44

WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1

# The tree is mounted read-only by compose; nothing is copied in, so a run
# always tests the working tree rather than a stale layer.
CMD ["python", "-m", "unittest", "discover", "-s", "tests/e2e", "-t", ".", "-v"]
