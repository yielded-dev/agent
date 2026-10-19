/** Trusted, read-only server. -I -S prevents imports from the workspace or site hooks. */
export const STATIC_PREVIEW_PORT = 4504;
export const STATIC_PREVIEW_HEALTH_PATH = "/__preview/health";

export const STATIC_PREVIEW_SERVER = String.raw`
import http.server
import mimetypes
import os
import socketserver
import stat
import sys
import urllib.parse

root = os.open('/workspace', os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
flags = os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC

class Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_GET(self):
        self.serve()

    def do_HEAD(self):
        self.serve()

    def refused(self):
        self.send_error(405, 'Only GET and HEAD are supported')

    do_POST = do_PUT = do_PATCH = do_DELETE = do_OPTIONS = do_CONNECT = do_TRACE = refused

    def serve(self):
        fd = None
        sent = False
        try:
            target = urllib.parse.urlsplit(self.path)
            # Decode once; remaining percent signs are literal filename characters.
            path = urllib.parse.unquote(target.path, encoding='utf-8', errors='strict')
            if target.scheme or target.netloc or not path.startswith('/'):
                raise ValueError()
            if any(ord(c) < 32 or ord(c) == 127 for c in path) or '\\' in path:
                raise ValueError()
            parts = [part for part in path.split('/') if part]
            if any(part in ('.', '..') or part.startswith('.sandbox-agent') for part in parts):
                raise ValueError()
            if path == '/__preview/health':
                self.send_response(204)
                self.send_header('X-Yielded-Static-Preview', sys.argv[1])
                self.end_headers()
                return
            fd = os.dup(root)
            for part in parts:
                child = os.open(part, flags, dir_fd=fd)
                os.close(fd)
                fd = child
            if stat.S_ISDIR(os.fstat(fd).st_mode):
                if not path.endswith('/'):
                    self.send_response(308)
                    self.send_header('Location', urllib.parse.quote(path, safe='/') + '/')
                    self.send_header('Content-Length', '0')
                    self.end_headers()
                    return
                child = os.open('index.html', flags, dir_fd=fd)
                os.close(fd)
                fd = child
                path += 'index.html'
            info = os.fstat(fd)
            if not stat.S_ISREG(info.st_mode):
                raise ValueError()
            self.send_response(200)
            self.send_header('Content-Type', mimetypes.guess_type(path)[0] or 'application/octet-stream')
            self.send_header('Content-Length', str(info.st_size))
            self.send_header('Cache-Control', 'no-store')
            self.send_header('X-Content-Type-Options', 'nosniff')
            self.end_headers()
            sent = True
            if self.command == 'GET':
                remaining = info.st_size
                while remaining:
                    block = os.read(fd, min(65536, remaining))
                    if not block:
                        break
                    self.wfile.write(block)
                    remaining -= len(block)
        except (BrokenPipeError, ConnectionResetError):
            pass
        except (OSError, ValueError, UnicodeError):
            if not sent:
                self.send_error(404, 'File unavailable')
        finally:
            if fd is not None:
                os.close(fd)

class Server(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

# TCPServer avoids HTTPServer's reverse DNS lookup in the container.
with Server(('0.0.0.0', ${STATIC_PREVIEW_PORT}), Handler) as server:
    server.serve_forever()
`;
