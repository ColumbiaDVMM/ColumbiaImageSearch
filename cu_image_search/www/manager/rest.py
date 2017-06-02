# https://docs.microsoft.com/en-us/rest/api/storageservices/common-rest-api-error-codes


def created(data={}):
    return data, 201


def ok(data=''):
    return data, 200


def deleted():
    return '', 204


def bad_request(data=''):
    return {'status_code': 400, 'error_message': data}, 400


def exists(data=''):
    return {'status_code': 409, 'error_message': data}, 409


def not_found(data=''):
    return {'status_code': 404, 'error_message': data}, 404


def internal_error(data=''):
    return {'status_code': 500, 'error_message': data}, 500
