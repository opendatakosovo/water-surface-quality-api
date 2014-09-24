import os
import ConfigParser
import logging
import logging.config

from logging.handlers import RotatingFileHandler

from flask import Flask
from flask.ext.pymongo import PyMongo


# Create MongoDB database object.
mongo = PyMongo()


def create_app():
    app = Flask(__name__)

    # Loads the config.cfg file.
    load_config(app)

    # Initializing logging
    configure_logging(app)

    # Register url rules
    register_url_rules(app)

    # Init app for use with this PyMongo
    # http://flask-pymongo.readthedocs.org/en/latest/#flask_pymongo.PyMongo.init_app
    mongo.init_app(app, config_prefix='MONGO')

    return app


def load_config(app):
    ''' Reads the config file and loads configuration properties into the Flask app.
    :param app: The Flask app object.
    '''

    # Get the path to the application directory, that's where the config file resides.
    par_dir = os.path.join(__file__, os.pardir)
    par_dir_abs_path = os.path.abspath(par_dir)
    app_dir = os.path.dirname(par_dir_abs_path)

    # Read config file
    # FIXME: Use the "common pattern" described in "Configuring from Files": http://flask.pocoo.org/docs/config/
    config = ConfigParser.RawConfigParser()
    config_filepath = app_dir + '/config.cfg'
    config.read(config_filepath)

    # Set up config properties
    app.config['SERVER_PORT'] = config.get('Application', 'SERVER_PORT')
    app.config['BASE_PATH'] = config.get('Application', 'BASE_PATH')

    app.config['MONGO_DBNAME'] = config.get('Mongo', 'DB_NAME')

    # Logging path might be relative or starts from the root.
    # If it's relative then be sure to prepend the path with the application's root directory path.
    log_path = config.get('Logging', 'PATH')
    if log_path.startswith('/'):
        app.config['LOG_PATH'] = log_path
    else:
        app.config['LOG_PATH'] = app_dir + '/' + log_path

    app.config['LOG_LEVEL'] = config.get('Logging', 'LEVEL').upper()


def configure_logging(app):

    # Get the path of the log from the config
    log_path = app.config['LOG_PATH']

    # Get the level of logging from the config
    log_level = app.config['LOG_LEVEL']

    # If path directory doesn't exist, create it.
    log_dir = os.path.dirname(log_path)
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    # Create formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # Create Log_Handler
    log_handler = RotatingFileHandler(log_path, maxBytes=250000, backupCount=5)

    # add formatter to log handler
    log_handler.setFormatter(formatter)

    # Get the level of the Debug and set it to the logger
    app.logger.setLevel(log_level)

    # Add the handlers to the logger
    app.logger.addHandler(log_handler)

    # Test if the logging is working by typing this string to a file.
    app.logger.info('Logging to: %s', log_path)

# Index page view.
from views.index import Index

from views.aggregate.measurements import Measurements

# Import Aggregate views for averages
from views.aggregate.average.river import MeasurmentsForARiver
from views.aggregate.average.riverstations import AverageStationMeasurementsForGivenRiver
from views.aggregate.average.everystation import AllAverageStationMeasurements


def register_url_rules(app):
    ''' Register the URL rules.
    Use pluggable class-based views: http://flask.pocoo.org/docs/views/
    :param app: the Flask application instance.
    '''

    # Show instructional index page.
    app.add_url_rule('/', view_func=Index.as_view('index'))

    app.add_url_rule(
        '/measurements',
        view_func=Measurements.as_view('measurements'))

    register_avg_url_rules(app)


def register_avg_url_rules(app):
    ''' Register url rules relating to getting the measurement averages.
    :param app: the Flask app object.
    '''
    app.add_url_rule(
        '/aggregate/average/river/<string:river_slug>',
        view_func=MeasurmentsForARiver.as_view(
            'average_measurments_for_all_stations_of_a_given_river'))

    app.add_url_rule(
        '/aggregate/average/river/<string:river_slug>/stations',
        view_func=AverageStationMeasurementsForGivenRiver.as_view(
            'average_measurments_for_each_station_of_a_given_river'))

    app.add_url_rule(
        '/aggregate/average/stations',
        view_func=AllAverageStationMeasurements.as_view(
            'average_measurments_of_every_station'))
