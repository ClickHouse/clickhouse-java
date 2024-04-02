package com.clickhouse.client.api.data_formats;

public class JSON extends DataFormat {

    public JSON() {
        super();
        setSetting("format_json_quote_64bit_integers", "true");
    }
}
