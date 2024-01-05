(function ($) {
    "use strict";

    // Validation and Ajax action
    $("form#login_form").validate({
        rules: {
            username: {
                required: true
            },

            userpassword: {
                required: true
            }
        },

        messages: {
            username: {
                required: '请输入您的用户名.'
            },

            userpassword: {
                required: '请输入您的密码.'
            }
        },

        // Form Processing via AJAX
        submitHandler: function (form) {
           
            
            $('#errorinfodiv').hide();
            $.ajax({
                url: "/login/",
                method: 'POST',
                //dataType: 'json',
                data: $('#login_form').serialize(),
                success: function (data) {
                    console.log("data:",data);
                    if (!data.errorFlag) {
                        $("#errorinfo").html(data.errorMsg);
                        $('#errorinfodiv').show();
                    } else {
                        var redirectUrl = data.data.redirectUrl;
                        window.location.href = redirectUrl;
                        return false;
                    }
                }, error: function (xhr, textStatus) {
                    console.log(xhr);
                    console.log(textStatus);
                }
            });

        }
    });

    // Set Form focus
    $("form#login_form .form-group:has(.form-control):first .form-control").focus();

})(jQuery);




