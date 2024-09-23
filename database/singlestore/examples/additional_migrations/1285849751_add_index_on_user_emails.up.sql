-- Taken from postgres example, but removed the
-- CONCURRENT keyword which is not valid in mysql
--
-- Also removed UNIQUE for singlestore cause it gets
-- fussy about demanding a shard key and it's not worth it for a test.
CREATE INDEX users_email_index ON users (email);

-- Lorem ipsum dolor sit amet, consectetur adipiscing elit. Aenean sed interdum velit, tristique iaculis justo. Pellentesque ut porttitor dolor. Donec sit amet pharetra elit. Cras vel ligula ex. Phasellus posuere.
