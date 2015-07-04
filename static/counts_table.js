var vm_data = new Array();
for (i =0;i< vm_js.length;i++){

	vm_data[i] = new Array()
    vm_data[i].push(vm_js[i],vm_c_js[i])

	}
$(document).ready(function() {
    $('#devicetable').html( '<table cellpadding="0" cellspacing="0" border="0" class="display" id="devicedata"></table>' );

    $('#devicedata').dataTable( {
        "data": device_js,
        "columns": [
            { "title": "Device" },
            { "title": "Count" }

        ]
    } );
    $('#vmtable').html( '<table cellpadding="0" cellspacing="0" border="0" class="display" id="vmdata"></table>' );

    $('#vmdata').dataTable( {
        "data": vm_data,
        "columns": [
            { "title": "Device" },
            { "title": "Count" }

        ]
    } );
} );


