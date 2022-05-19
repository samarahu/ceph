# -*- coding: utf-8 -*-
import cherrypy


class ViewCacheNoDataException(Exception):
    def __init__(self):
        self.status = 200
        super(ViewCacheNoDataException, self).__init__('ViewCache: unable to retrieve data')


class DashboardException(Exception):
    """
    Used for exceptions that are already handled and should end up as a user error.
    Or, as a replacement for cherrypy.HTTPError(...)

    Typically, you don't inherent from DashboardException
    """

    # pylint: disable=too-many-arguments
    def __init__(self, e=None, code=None, component=None, http_status_code=None, msg=None):
        super().__init__(msg)
        self._code = code
        self.component = component
        if e:
            self.e = e
        if http_status_code:
            self.status = http_status_code
        else:
            self.status = 400

    def __str__(self):
        try:
            return str(self.e)
        except AttributeError:
            return super().__str__()

    def as_cherrypy_exception(self):
        return cherrypy.HTTPError(status=self.status, message=str(self))

    @property
    def errno(self):
        return self.e.errno

    @property
    def code(self):
        if self._code:
            return str(self._code)
        return str(abs(self.errno)) if self.errno is not None else 'Error'


class InvalidCredentialsError(DashboardException):
    def __init__(self):
        super().__init__(msg='Invalid credentials',
                         code='invalid_credentials',
                         http_status_code=401,
                         component='auth')


class RequestError(DashboardException):
    def __init__(self, code, msg, component):
        super().__init__(code, msg, component, http_status_code=400)


# access control module exceptions
class RoleAlreadyExists(Exception):
    def __init__(self, name):
        super(RoleAlreadyExists, self).__init__(
            "Role '{}' already exists".format(name))


class RoleDoesNotExist(Exception):
    def __init__(self, name):
        super(RoleDoesNotExist, self).__init__(
            "Role '{}' does not exist".format(name))


class ScopeNotValid(Exception):
    def __init__(self, name):
        super(ScopeNotValid, self).__init__(
            "Scope '{}' is not valid".format(name))


class PermissionNotValid(Exception):
    def __init__(self, name):
        super(PermissionNotValid, self).__init__(
            "Permission '{}' is not valid".format(name))


class RoleIsAssociatedWithUser(Exception):
    def __init__(self, rolename, username):
        super(RoleIsAssociatedWithUser, self).__init__(
            "Role '{}' is still associated with user '{}'"
            .format(rolename, username))


class UserAlreadyExists(Exception):
    def __init__(self, name):
        super(UserAlreadyExists, self).__init__(
            "User '{}' already exists".format(name))


class UserDoesNotExist(Exception):
    def __init__(self, name):
        super(UserDoesNotExist, self).__init__(
            "User '{}' does not exist".format(name))


class ScopeNotInRole(Exception):
    def __init__(self, scopename, rolename):
        super(ScopeNotInRole, self).__init__(
            "There are no permissions for scope '{}' in role '{}'"
            .format(scopename, rolename))


class RoleNotInUser(Exception):
    def __init__(self, rolename, username):
        super(RoleNotInUser, self).__init__(
            "Role '{}' is not associated with user '{}'"
            .format(rolename, username))


class PwdExpirationDateNotValid(Exception):
    def __init__(self):
        super(PwdExpirationDateNotValid, self).__init__(
            "The password expiration date must not be in the past")


class GrafanaError(Exception):
    pass


class PasswordPolicyException(Exception):
    pass
