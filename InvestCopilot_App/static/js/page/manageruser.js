$(function () {
    getAllUserList();

    $("#addUserBtn").click(function () {
        $("#myModalLabel").text("添加用户");
        $("#btn_addUser").show();
        $("#btn_editUser").hide();

        $("#editUserId").val('');
        $("#userEmail").val('');
        $("#userRealName").val('');
        $("#userNickName").val('');
        
        $("#userRoleList").select2("val", "");
    
        $("#userEmail").removeAttr("readonly");
        $("#userRealName").removeAttr("readonly");
        $("#userNickName").removeAttr("readonly");
        $("#addUserModal").modal('show');
    });

    $("#flushCacheBtn").click(function () {
        flushUserCache();
    });

    //Select Init
    $("#userCompany").select2({
        placeholder: '请选择公司',
        allowClear: true
    }).on('select2-open', function () {
        // Adding Custom Scrollbar
        $(this).data('select2').results.addClass('overflow-hidden').perfectScrollbar();
    });

    //Select Init
    $("#userGroup").select2({
        placeholder: '输入权限组名称(可选)',
        allowClear: true
    }).on('select2-open', function () {
        // Adding Custom Scrollbar
        $(this).data('select2').results.addClass('overflow-hidden').perfectScrollbar();
    });

    //Select Init
    var userRoleSelect = $("#userRole").select2({
        placeholder: '请选择角色',
        allowClear: true
    });
    userRoleSelect.on('change', function (e) {
        $("#btn_viewRole").attr("btnRoleId", userRoleSelect.val());
        $("#btn_viewRole").text(e.added['text']);
    })
});


function view_roleInfo() {
    var roleId = $("#btn_viewRole").attr('btnRoleId');
    $("#viewRoleId").val(roleId);
    $("#managerMenuForm").submit();
}

function toeditUser(obj, roleId) {
    $("#myModalLabel").text("编辑用户");
    $("#btn_addUser").hide();
    $("#btn_editUser").show();

    var userId = $.trim($(obj).parent('td').parent('tr').children().eq(0).text());
    var userName = $.trim($(obj).parent('td').parent('tr').children().eq(1).text());
    var realName = $.trim($(obj).parent('td').parent('tr').children().eq(2).text());
    var nickName = $.trim($(obj).parent('td').parent('tr').children().eq(3).text());
    var roleName = $.trim($(obj).parent('td').parent('tr').children().eq(4).text());
    
    $("#editUserId").val(userId);
    $("#userEmail").val(userName);
    $("#userEmail").attr("readonly", "readonly");
    $("#userRealName").val(realName);
    $("#userRealName").attr("readonly", "readonly");
    $("#userNickName").val(nickName);
    $("#userNickName").attr("readonly", "readonly");
    
    let curUserRoleList = roleId.split(',');
    
    $("#userRoleList").select2("val",curUserRoleList);
    
    $("#btn_viewRole").attr("btnRoleId", roleId);
    $("#btn_viewRole").text(roleName);

    $("#addUserModal").modal('show');
}


function toRestUserPwd(obj, userId) {
    $("#newPwd").val('');
    $("#newPwdAgain").val('');
    $("#restPwdUserId").val(userId);
    $("#restUserPwdModal").modal('show');
}

//获取用户列表
function getAllUserList() {
    
    $.ajax({
        type: "POST",
        url: "/api/getAllUserData/",
        data: {},
        success: function (result) {
            
            if (result.errorFlag == false) {
                console.log(result.errorMsg);
                return;
            }
            var columnsArray = new Array();
            var columns = result.data.tbColumn;
            for (var idx in columns) {
                columnsArray.push({ 'title': columns[idx], 'class': 'center' });
            }
            var datas = result.data.tbData;
            $('#userListTb').dataTable().fnClearTable();   //将数据清除
            $('#userListTb').dataTable().fnDestroy();   //将数据清除
            $('#userListTb').empty();   //将数据清除
            var table = $('#userListTb').dataTable({
                "bAutoWidth": false,
                language: {
                    url: '/api/dataTableLanguage'
                },
                "order": [],
                "destroy": true,
                "sort": true,
                "searching": true,
                "paging": true,
                "info": true,
                "columns": columnsArray,
                "data": datas,
                "pageLength": 10,
                "columnDefs": [
                    {
                        "targets": [-1],
                        "render": function (data, type, full) {
                            //var btnHtml = '<button class="btn btn-primary btn-sm" style="height:23px;padding-top:1px; margin-bottom:5px; padding-left:5px;padding-right:5px;" onclick="stopUser(this);">停用</button>' +
                            //        '&nbsp;&nbsp;' +
                            //        '<button class="btn btn-primary btn-sm" style="height:23px;padding-top:1px;margin-bottom:5px; padding-left:5px;padding-right:5px;" onclick="startUser(this);">启用</button>';
                            var btnHtml = '<button class="btn btn-primary btn-sm" style="height:23px;padding-top:1px;margin-bottom:5px; padding-left:5px;padding-right:5px;margin-right:5px;" onclick="toeditUser(this,\'' + full[0] + '\');">编辑</button>';
                            var restPwd = '<button class="btn btn-primary btn-sm" style="height:23px;padding-top:1px;margin-bottom:5px; padding-left:5px;padding-right:5px;" onclick="toRestUserPwd(this,\'' + full[1] + '\');">重置密码</button>';
                            return btnHtml + restPwd;
                        }
                    },
                    { "visible": false, "targets": [0] }
                ]
            });
        }
    });
}

//添加用户
function addUser() {
    var email = $("#userEmail").val();
    var userRealName = $("#userRealName").val();
    var userNickName = $("#userNickName").val();
    var roleId = $("#userRoleList").val();
    console.log(roleId);
    if (email.length == 0) {
        bootbox.alert("用户名不能为空！");
        return;
    }
    if (userRealName.length == 0) {
        bootbox.alert("真实姓名不能为空！");
        return;
    }
    if (userNickName.length == 0) {
        bootbox.alert("用户昵称不能为空！");
        return;
    }
    if (roleId.length == 0) {
        bootbox.alert("请选择角色！");
        return;
    }
    var companyId = '';
    var groupName = '';

    $.ajax({
        type: "POST",
        url: "/api/addUser/",
        data: {
            'email': email,
            'userRealName': userRealName,
            'userNickName': userNickName,
            'companyId': companyId,
            'groupName': groupName,
            'roleId': roleId
        },
        success: function (result) {
            if (result.errorFlag == false) {
                if (result.errorCode == '0000005') {
                    bootbox.alert(result.errorMsg);
                } else {
                    console.log(result.errorMsg);
                }
                return;
            }
            $("#addUserModal").modal('hide');
            var userName = result.data.username;
            var password = result.data.password;
            bootbox.alert('<p>用户名：' + userName + '</p><p>密码：' + password + '</p>', function () {
                window.location.href = "manageuser.html";
            });
        }
    });
}

//修改用户角色
function editUserRole() {
    var editUserId = $("#editUserId").val();
    var roleId = $("#userRoleList").val();
    console.log(roleId);
    if (editUserId.length == 0) {
        bootbox.alert("请选择需要修改角色的用户！");
        return;
    }
    if (roleId.length == 0) {
        bootbox.alert("请选中需要修改的用户角色！");
        return;
    }
    $.ajax({
        type: "POST",
        url: "/api/editUserRole/",
        data: {
            'editUserId': editUserId,
            'roleId': roleId
        },
        success: function (result) {
            if (!result.errorFlag) {
                bootbox.alert(result.errorMsg);
                return;
            } else {
                //toastr.info("修改用户角色成功！");
                getAllUserList();
            }
            $("#addUserModal").modal('hide');
        }
    });
}

//刷新用户缓存
function flushUserCache() {
    $.ajax({
        type: "POST",
        url: "/api/flushUserCache/",
        data: {},
        success: function (result) {
            if (result.errorFlag == false) {
                console.log(result.errorMsg);
                return;
            }
            bootbox.alert('刷新缓存成功！');
        }
    });
}

//修改用户密码
function restPwd() {
    $.ajax({
        type: "POST",
        url: "/api/restUserPwd/",
        data: { "userId": $("#restPwdUserId").val(), "newPwd": $("#newPwd").val(), "newPwdAgain": $("#newPwdAgain").val() },
        success: function (result) {
            console.log(result);
            bootbox.alert(result.errorMsg);
            $("#restUserPwdModal").modal('hide');
        }
    });
}
