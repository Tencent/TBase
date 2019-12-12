Intro
=====

This module is heavily inspired by the pam-http module by Kragen Sitaker. I rewrote it largely because I wanted to MIT license it (instead of GPL) and because there was some profanity in the source.  Also, the version I modeled this off of didn't even compile because it used an old version of libcurl.

This works with libcurl v. 7.21.3 (the one in Ubuntu's repositories).

To build, just type `make`. It will create `mypam.so` and a `test` executable.

Simple Usage
------------

The .so file should be put in `/lib/security` and the PAM config files will need to be edited accordingly.

The config files are located in `/etc/pam.d/` and the one I changed was `/etc/pam.d/common-auth`. This is NOT the best place to put it, as sudo uses this file and you could get unexpected results (like an HTTP user could gain root access; cool huh?).

Put something like this in one of the config files (change the URL to whatever you like):

	auth sufficient mypam.so url=https://localhost:2000
	account sufficient mypam.so

Sufficient basically means that if this authentication method succeeds, the user is given access.

To test, run the test program with a single argument, the username. I have provided a sample HTTPS server (you'll need your own certificate) that will accept all usernames and passwords. This module does not check the validity of certificates, so a custom one will do.
