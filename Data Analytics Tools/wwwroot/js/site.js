// Please see documentation at https://docs.microsoft.com/aspnet/core/client-side/bundling-and-minification
// for details on configuring this project to bundle and minify static web assets.

// Write your JavaScript code.


function LoadWithProgressModal(url, loaderModal)
{
    $(loaderModal).show();

    $.ajax({
        url: url,
        type: 'GET',
        success: function (res)
        {
            toastr.success("Success");
            $(loaderModal).hide();
            window.location.reload(); 
        },
        error: function (res)
        {
            toastr.error("Error");
            $(loaderModal).hide();
            window.location.reload(); 
        }
    });
}
