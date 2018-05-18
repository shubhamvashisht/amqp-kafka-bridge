package io.strimzi.kafka.bridge.http;

/**
 * The HTTP side working mode (client or server)
 */
public enum HttpMode {

        CLIENT("client"),
        SERVER("server");

        private final String mode;

        private HttpMode(String mode) {
            this.mode = mode;
        }

        /**
         * Get the enum value from the corresponding string value
         *
         * @param mode  mode as a string
         * @return  mode as enum value
         */
        public static HttpMode from(String mode) {
            if (mode.equals(CLIENT.mode)) {
                return CLIENT;
            } else if (mode.equals(SERVER.mode)) {
                return SERVER;
            } else {
                throw new IllegalArgumentException("Unknown mode: " + mode);
            }
        }
}
