# AiryFS demos

One tiny script per capability, in the "show a friend" spirit. Each has the
feature it demonstrates in a leading comment and the real output (captured on
the integration environment) in trailing comments.

First point a session at your deployment (once):

    airy session create demo -e https://airyfs-int.tyson-s-sandbox.workers.dev -v airy-demo

Every script then just runs `airy ...` against that active session. Volume names
in the captured output are cosmetic.
