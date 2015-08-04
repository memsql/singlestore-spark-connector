import os
import pipes
import spur
import paramiko

BIN_PATHS = ["/bin", "/usr/bin", "/usr/local/bin", "/sbin", "/usr/sbin", "/usr/local/sbin"]

SSHException = paramiko.SSHException
ConnectionError = spur.ssh.ConnectionError
CommandInitializationError = spur.errors.CommandInitializationError
MissingHostKey = spur.ssh.MissingHostKey
NoSuchCommandError = spur.errors.NoSuchCommandError
RunProcessError = spur.results.RunProcessError
ExecutionResult = spur.results.ExecutionResult

class ShellMixin(object):
    def run(self, *args, **kwargs):
        decode_output = kwargs.pop('decode_output', True)
        strip_output = kwargs.pop('strip_output', True)

        if "update_env" in kwargs:
            kwargs["update_env"].update(self._get_env_variables())
        else:
            kwargs["update_env"] = self._get_env_variables()

        resp = super(ShellMixin, self).run(*args, **kwargs)

        if decode_output:
            resp.output = decode_unicode(resp.output)
            resp.stderr_output = decode_unicode(resp.stderr_output)

        if strip_output:
            resp.output = resp.output.strip()
            resp.stderr_output = resp.stderr_output.strip()

        return resp

    def spawn(self, *args, **kwargs):
        if "update_env" in kwargs:
            kwargs["update_env"].update(self._get_env_variables())
        else:
            kwargs["update_env"] = self._get_env_variables()

        return super(ShellMixin, self).spawn(*args, **kwargs)

    def sudo(self, command, password, **kwargs):
        # If we're the root user, don't use sudo at all.  This allows us to use
        # this function on systems that don't have the sudo command.
        if self._is_root():
            return self.run(command, **kwargs)
        if password is None:
            return self.run([ "sudo", "-n" ] + command, **kwargs)
        command_str = " ".join(pipes.quote(s) for s in command)
        return self.run([ "/bin/bash", "-c", "echo '%s' | sudo -S %s" % (password, command_str) ], **kwargs)

    def _get_env_variables(self):
        names = []

        return { name: os.environ[name]
                 for name in names if name in os.environ }

    def _is_root(self):
        try:
            current_uid_out = self.run([ "id", "-u" ], allow_error=True)
            if current_uid_out.return_code == 0:
                return current_uid_out.output == "0"
        except NoSuchCommandError:
            pass
        return False

class SSHShell(ShellMixin, spur.ssh.SshShell):
    def __init__(self, **kwargs):
        if "missing_host_key" not in kwargs:
            kwargs["missing_host_key"] = MissingHostKey.accept

        return super(SSHShell, self).__init__(**kwargs)

class LocalShell(ShellMixin, spur.local.LocalShell):
    pass

def decode_unicode(s):
    try:
        return str(s.decode("utf-8"))
    except UnicodeDecodeError:
        return str(s)
