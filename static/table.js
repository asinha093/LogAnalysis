
$(document).ready(function() {
    $('#demo').html( '<table cellpadding="0" cellspacing="0" border="0" class="display" id="example"></table>' );

    $('#example').dataTable( {
        "data": data_js,
        "columns": [
            { "title": data_header_js[0] },
            { "title": data_header_js[1] },
            { "title": data_header_js[2] },
            { "title": data_header_js[3] },
            { "title": data_header_js[4] },
            { "title": data_header_js[5] },
            { "title": data_header_js[6] },
            { "title": data_header_js[7] },
            { "title": data_header_js[8] },
            { "title": data_header_js[9] , "class": "center" }
        ]
    } );
} );