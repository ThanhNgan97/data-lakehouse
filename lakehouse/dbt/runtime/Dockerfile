FROM python:3.11.14-slim@sha256:c8271b1f627d0068857dce5b53e14a9558603b527e46f1f901722f935b786a39

WORKDIR /runtime
COPY requirements.txt requirements.lock.txt ./
RUN pip install --no-cache-dir -r requirements.lock.txt && pip check
COPY preflight.py ./
ENV DBT_SEND_ANONYMOUS_USAGE_STATS=false PYTHONUNBUFFERED=1
# Step 1 only: the job API and business project are not installed yet.
CMD ["sleep", "infinity"]
