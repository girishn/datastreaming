FROM public.ecr.aws/lambda/python:3.12

# Install dependencies into the Lambda task root
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt -t "${LAMBDA_TASK_ROOT}"

# Copy function code
COPY app.py ${LAMBDA_TASK_ROOT}

# Lambda entrypoint (module.function)
CMD ["app.handler"]

