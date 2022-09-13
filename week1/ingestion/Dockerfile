FROM python:3.9

WORKDIR /code
COPY ./requirements.txt /code/requirements.txt
#COPY ./pipeline.py /code/pipeline.py
RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt
EXPOSE 8889
CMD ["jupyter", "notebook", "--port=8889", "--no-browser", "--ip=0.0.0.0", "--allow-root"]