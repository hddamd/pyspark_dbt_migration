FROM jupyter/pyspark-notebook

# Create working directory
WORKDIR /workspace

# copy data and notebook
COPY data/ ./data
COPY pyspark/main.ipynb .

# install needed libs
# RUN pip install --no-cache-dir -r requirements.txt