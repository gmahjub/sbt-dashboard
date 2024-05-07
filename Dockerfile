# Use the official lightweight Python image
# https://hub.docker.com/_/python
FROM python:3.11

# copy local code to the container image
ENV APP_HOME /my_app
WORKDIR $APP_HOME
COPY . ./

# install production dependencies.
RUN pip install -r requirements.txt

EXPOSE 8080
#EXPOSE $PORT

CMD python my_app.py