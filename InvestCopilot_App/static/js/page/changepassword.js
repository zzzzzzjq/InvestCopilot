
function clearinput(){
    $("#oldpasswd").val('');
    $("#newpasswd1").val('');
    $("#newpasswd2").val('');
}

function confirm(){		    
    var oldpasswd = $("#oldpasswd").val();
    var newpasswd1 = $("#newpasswd1").val();
    var newpasswd2 = $("#newpasswd2").val();
    
    if (newpasswd1!=newpasswd2)
    {
        $("#InfoTitle").html('请检查：'); 
        $("#InfoContent").html('新密码两次输入不一致！请重新输入。');
        $("#Infomodal-1").modal();
        return;
    };

    
    $.ajax({
        url: "/api/PasswordChange/",
        method: 'POST',
        //dataType: 'json',
        data: {
            'oldpasswd': oldpasswd,
            'newpasswd1': newpasswd1,
            'newpasswd2': newpasswd2
        },				
        success: function (data) {
            console.log(data);
            $("#InfoTitle").html('处理结果：');
            $("#InfoContent").html(data.errorMsg);
            $("#Infomodal-1").modal();
            return;
        },
        error: function (xhr, textStatus) {
            $("#InfoTitle").html('Error：');
            $("#InfoContent").html('抱歉，处理过程失败，请稍后重试');
            $("#Infomodal-1").modal();
            console.log('错误');
            console.log(xhr);
            console.log(textStatus);
        }
    });

}