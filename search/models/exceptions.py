"""Exceptions for the search API"""


class SearchIndexError(Exception):
    """Exception class when something goes wrong with looking for a index"""


class ConnectivityError(Exception):
    """Exception class when something goes wrong with looking for a index"""


class MissingDocumentTypeError(Exception):
    """Need to specify a document type"""
