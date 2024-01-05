/******************************/
/*      openAI任务管理代码     */
/*        JS V0.0.1           */
/*     Create by  Rocky       */
/*      in 20231125           */
/******************************/

/******************************/
/*     全局变量(部分在页面定义) */
/******************************/
//toastr 冒泡提示框，对一些弹出框的提示进行统一管理
var toastr;

/******************************/
/*        基础控件初始化       */
/******************************/
function init_base_control() {

    //初始化toastr
    initToastr();
    
    /*jQuery.validator.addMethod("check_userportfolio_name", function (value, element) {
        let isexist = false;
        
        for (let i = 0; i < userportfolio.length; i++) {
            if (value == userportfolio[i].portfolioname) {
                isexist = true;
                break;
            }
        }
        if (isexist) {
            $(element).data('error-msg', '组合名称：' + value + ' 已经存在!');            
            return false;
        }        
        return !isexist;
    }, function (params, element) {
        return $(element).data('error-msg');
    });    

    $("#form_new_userportfolio").validate({
        onfocusout: function (element) {
            let result = $(element).valid();
            if (!result){
                $("#submit_userportfolio").attr("disabled", "disabled");
            }
            else
            {
                $("#submit_userportfolio").removeAttr("disabled");
            }
        },
        rules: {
            new_portfolio_name: {
                required: true,
                check_userportfolio_name: true,
            }
        },
        messages: {
            new_portfolio_name: {
                required:"请输入组合名称",
                //check_userportfolio_name:"组合名称已经存在",
            }
        }        
    });    


    //初始化组合列表
    refresh_userportfolio_list();*/
}


function initToastr() {
    toastr.options = {
        "closeButton": false,
        "debug": false,
        "newestOnTop": false,
        "progressBar": false,
        "positionClass": "toast-top-center",
        "preventDuplicates": false,
        "onclick": null,
        "showDuration": "300",
        "hideDuration": "1000",
        "timeOut": "5000",
        "extendedTimeOut": "1000",
        "showEasing": "swing",
        "hideEasing": "linear",
        "showMethod": "fadeIn",
        "hideMethod": "fadeOut"
    };
}

/******************************/
/*        界面交互类事件       */
/******************************/
//新建用户组合点击事件
function click_new_portfolio() {    
    $("#modal_new_portfolio").modal("show");
}
//添加股票点击事件
function click_add_stock() {
  $("#modal_add_stock").modal("show");
}
//新建用户组合提交事件
function submit_new_userportfolio()
{
    //Disable submit button
    $("#submit_userportfolio").attr("disabled", "disabled");    
    //获取新建组合名称
    let new_userportfolio = $("#new_portfolio_name").val();
    console.log(new_userportfolio);
    //提交到后台
    $.ajax({
        url: "/api/proc_userportfolio/",
        method: 'POST',
        cache: false,
        data: {
            action: "new_userportfolio",
            new_userportfolio: new_userportfolio
        },
        success: function (data) {
            console.log(data);
            //errorCode: '0000000', errorFlag: true, errorMsg: '', returnCode: 100, returnMsg: '添加成功！'
            if (data.returnCode == '100' && data.errorCode == '0000000') {                
                //添加成功，刷新组合列表
                toastr.success(data.returnMsg, "成功"); //成功提示框        
                $("#modal_new_portfolio").modal("hide");
                getalluserportfolio();                
            }
            else {
                toastr.error(data.returnMsg, "失败"); //失败提示框                
            }
        },
        complete: function () {
            //$("#vloadingDiv0").hide();
        },
        error: function (xhr, textStatus) {
            $("#InfoTitle").html('服务器处理失败');
            $("#InfoContent").html('抱歉，服务器处理过程失败，请稍后重试');
            $("#Infomodal-1").modal();
            console.log('错误');
            console.log(xhr);
            console.log(textStatus);
        }
    });
}
function getalluserportfolio() {
    $.ajax({
        url: "/api/proc_userportfolio/",
        method: 'POST',
        cache: false,
        data: {
            action: "get_all_userportfolio",            
        },
        success: function (data) {
            console.log(data);
            //errorCode: '0000000', errorFlag: true, errorMsg: '', data0:[], column0:[]
            if (data.errorCode == '0000000') {
                //获取成功，刷新组合列表
                userportfolio = [];
                for (let i = 0; i < data.data0.length; i++) {
                    let portfolioinfo = {
                        portfolioid: data.data0[i][0],                        
                        userid : data.data0[i][1],
                        portfolioname: data.data0[i][2],
                        stocknum: data.data0[i][3],
                        stocklist: data.data0[i][4]
                    }
                    userportfolio.push(portfolioinfo);
                }
                refresh_userportfolio_list();
            }
            else {
                toastr.error(data.returnMsg, "失败"); //失败提示框                
            }
        },
        complete: function () {
            //$("#vloadingDiv0").hide();
        },
        error: function (xhr, textStatus) {
            $("#InfoTitle").html('服务器处理失败');
            $("#InfoContent").html('抱歉，服务器处理过程失败，请稍后重试');
            $("#Infomodal-1").modal();
            console.log('错误');
            console.log(xhr);
            console.log(textStatus);
        }
    });
}

//刷新组合列表
function refresh_userportfolio_list() {
    let html = '<li><button class="no_border_button" type = "button"><div class="row div_button_info1"><div class="col-md-12 "><span class="portfolio_name">默认组合</span></div></div>';
    html = html + '<div class="row div_button_info2"><div class="col-md-12"><span class="portfolio_desc">共4个股票</span></div></div></button ></li >';
    let htmlend = '<li class="divider"></li><li><div class="div_selectbutton"><button class="btn btn-xs in_select_button" onclick="click_new_portfolio()">';
    htmlend = htmlend + '<i class="fa fa-plus"></i>新建组合</button></div></li>';
    for (let i = 0; i < userportfolio.length; i++) {
        html += '<li><button class="no_border_button" type = "button" onclick="click_portfolio(\'' + userportfolio[i].portfolioid + '\',\'' + userportfolio[i].portfolioname +'\')"><div class="row div_button_info1"><div class="col-md-12"><span class="portfolio_name">' + userportfolio[i].portfolioname + '</span></div></div>';
        html = html + '<div class="row div_button_info2"><div class="col-md-12"><span class="portfolio_desc">共' + userportfolio[i].stocknum + '个股票</span></div></div></button ></li >';        
        /*
        html += "<li class='list-group-item list-group-item-action' onclick='click_userportfolio(this)'>" + userportfolio[i] + "</li>";
        */
    }
    html = html + htmlend;    
    $("#userportfolio_list").html(html);
}
function click_portfolio(portfolioid,portfolioname) {
    console.log(portfolioid);
    $("#selectedportfolio").html(portfolioname);
    $("#selectedportfolioid").val(portfolioid);
    getuserportfoliodetail(portfolioid);

}
//获取任务列表
function getalltaskinfo()
{
    $.ajax({
        url: "/api/procOpenAITaskInfo/",
        method: 'POST',
        cache: false,
        data: {
            doMethod: "getOpenAITaskInfo",            
        },
        success: function (data) {
            console.log(data);
            //errorCode: '0000000', errorFlag: true, errorMsg: '', data0:[], column0:[]
            if (data.errorCode == '0000000') {
                //获取成功，刷新任务列表
                setTableData1(data.data.data0, data.data.columns0);
            }
            else {
                toastr.error(data.returnMsg, "失败"); //失败提示框                
            }
        },
        complete: function () {
            //$("#vloadingDiv0").hide();
        },
        error: function (xhr, textStatus) {
            $("#InfoTitle").html('服务器处理失败');
            $("#InfoContent").html('抱歉，服务器处理过程失败，请稍后重试');
            $("#Infomodal-1").modal();
            console.log('错误');
            console.log(xhr);
            console.log(textStatus);
        }
    });
}

//获取openai队列
function getallqueueinfo() {
    $.ajax({
        url: "/api/procOpenAITaskInfo/",
        method: 'POST',
        cache: false,
        data: {
            doMethod: "getOpenAIQueueInfo",
        },
        success: function (data) {
            console.log(data);
            //errorCode: '0000000', errorFlag: true, errorMsg: '', data0:[], column0:[]
            if (data.errorCode == '0000000') {
                //获取成功，刷新任务列表
                setTableData2(data.data.data0, data.data.columns0);
            }
            else {
                toastr.error(data.returnMsg, "失败"); //失败提示框                
            }
        },
        complete: function () {
            //$("#vloadingDiv0").hide();
        },
        error: function (xhr, textStatus) {
            $("#InfoTitle").html('服务器处理失败');
            $("#InfoContent").html('抱歉，服务器处理过程失败，请稍后重试');
            $("#Infomodal-1").modal();
            console.log('错误');
            console.log(xhr);
            console.log(textStatus);
        }
    });
}

//显示任务列表  
function setTableData1(tableData, tableColumns) {
    //特殊设置
    //1、不显foot和head如果domstr为空，下拖动条不会出现，需要手动加上    
    var domstr = "<'row'<'dataTables_header'<'rightInfo'f>r>>t<'row'<'dataTables_footer clearfix'>>";        
    if (tableData.length > 10) {
        domstr = "<'row'<'dataTables_header'<'col-md-2'l><'col-md-3'B><'rightInfo'f>r>>t<'row'<'dataTables_footer clearfix'<'col-md-6'i><'col-md-6'p>>>";
    }

    //2、排序列,默认不排序
    let ordercolumns = [];

    //3、列名称
    let columnsArray = new Array();
    for (var idx in tableColumns) {
        /*if (in_array(tableColumns[idx],spField) == false) {
            columnsArray.push({ 'title': tableColumns[idx] + '&nbsp;&nbsp;', 'class': 'center' });
        }
        else
        {
            columnsArray.push({
                'title': tableColumns[idx] + '&nbsp;<i class="fa-question-circle big-fa" data-toggle="tooltip" data-placement="bottom" data-trigger="hover" \
                    data-container="body" data-original-title="' +fieldComment[tableColumns[idx]] +'"></i>&nbsp;&nbsp;&nbsp;&nbsp;', 'class': 'center' });
        }*/
        columnsArray.push({ 'title': tableColumns[idx] + '&nbsp;&nbsp;', 'class': 'center' });
    }

    //4、固定列
    let fixcolumns = { "leftColumns": 1, }; //固定左边第一列
    
    //5、列定义，包括格式化，隐藏，BAR显示等
    let columnsDefArray = new Array();
    let formatcolumns = [];
    let percentagecolumns = [];
    let nomarlformatcolumns = [];
    let hiddencolumns = [];
    let barColumns = [];
    let maxData = [];
    
    for (i = 0; i < tableColumns.length; i++) {
        if (tableColumns[i] == '暂不用') {
            //默认排序列
            //ordercolumns = [[i, 'desc']];
            barColumns.push(i);
            maxData.push(0);
        }
        else if (tableColumns[i] == '价格' || tableColumns[i] == 'PE' ) {
            formatcolumns.push(i);
        }
        else if (tableColumns[i].indexOf('变动') != -1) {
            percentagecolumns.push(i);
            //nomarlformatcolumns.push(i);
        }
        /*else if (in_array(tableColumns[i].indexOf('PE') != -1 || tableColumns[i].indexOf('PB') != -1 || tableColumns[i].indexOf('PS') != -1
            || tableColumns[i].indexOf('ROE') != -1)) {
            nomarlformatcolumns.push(i);
        }*/
    }

    columnsDefArray.push(
        {
            "targets": barColumns,
            "render": function (data, type, full, meta) {
                let data_index = meta.col;
                if (tableColumns[data_index] == '仓位(%)') {
                    let maxdataindex = in_arrayPos(data_index, barColumns);
                    return trBar(maxdataindex, maxData, data, '', 'left', 2, '#409ecc');
                }

                /*if (data_index == 1) {
                    
                }
                else if (in_array(data_index, maxDataArray1)) {

                    //return trBar(data_index, maxData, data, '', 'left', 2, '#409ecc');
                    return trBar(1, maxData, data, '', 'left', 2, '#8da0b6');
                }
                else if (in_array(data_index, maxDataArray2)) {
                    return trBar(2, maxData, data, '', 'center', 2);
                }*/

            }
        }
    );

    /*
    Orange
    Hex code: #FF9838
    RGB Value:  (255,152,56)

    Red
    Hex Code: #DC4E57
    RGB Value: (220,78,87)

    Yellow
    Hex code: #FDF4BF
    RGB Value: (255,244,191)

    Green
    Hex code: #C5E1A5
    RGB Value: (197,225,165)

    Blue
    Hex code: #4dcaf7
    Hex code2: #409ecc
    RGB Value: (77,202,247)
    */


    //百分比显示数据    
    columnsDefArray.push(
        {
            "targets": percentagecolumns,
            "render": function (data, type, full, meta) {
                return percentage(data, 2);
            }
        }
    );
    //格式化显示数据
    columnsDefArray.push(
        {
            "targets": formatcolumns,
            "render": function (data, type, full, meta) {
                let data_index = meta.col;
                if (tableColumns[data_index] == '持仓数量') {
                    return format(data, 0);
                }
                return format(data);
            }
        }
    );
    //格式化显示数据
    columnsDefArray.push(
        {
            "targets": nomarlformatcolumns,
            "render": function (data, type, full, meta) {
                return (data.toFixed(2));
            }
        }
    );
    columnsDefArray.push(
        {
            "targets": hiddencolumns,
            "visible": false
        }
    );

    
   


    let tableName = 'dataTable1';
    $('#' + tableName).dataTable().fnClearTable();   //将数据清除
    $('#' + tableName).dataTable().fnDestroy();   //将数据清除
    $('#' + tableName).empty();   //将数据清除  //清除列名,需要加载后台传入列名时需要
    $('#' + tableName).dataTable({
        "scrollX": true ,
        /*"sScrollX": "100%",        
        "bScrollCollapse": false,
        "sScrollXInner": "120%",*/
        "bAutoWidth": true,
        "autoWidth": true,
        //"colReorder": false,   //禁止列拖动
        "colReorder": true,  //可以水平拖动列  这个与柱状图有冲突，如果有柱状图，这个必须为false        
        "language": {
            "sProcessing": "处理中...",
            "sLengthMenu": "显示 _MENU_ 项结果",
            "sZeroRecords": "没有匹配结果",
            "sInfo": "显示第 _START_ 至 _END_ 项结果，共 _TOTAL_ 项",
            "sInfoEmpty": "显示第 0 至 0 项结果，共 0 项",
            "sInfoFiltered": "(由 _MAX_ 项结果过滤)",
            "sInfoPostFix": "",
            "sSearch": "",
            "searchPlaceholder": "搜索",
            "sUrl": "",
            "sEmptyTable": "表中数据为空",
            "sLoadingRecords": "载入中...",
            "sInfoThousands": ",",
            "oPaginate": {
                "sFirst": "首页",
                "sPrevious": "上页",
                "sNext": "下页",
                "sLast": "末页"
            },
            "oAria": {
                "sSortAscending": ": 以升序排列此列",
                "sSortDescending": ": 以降序排列此列"
            }
        },
        "destroy": true,
        "pageLength": 10,  
        "bStateSave": false,   //不保存状态
        "dom": domstr,
        "order": ordercolumns,
        //"dom" :"lBfrtip",    
        /*"buttons": [{
            text: 'Copy',
            "extend": 'copyHtml5',
            "exportOptions": {
                "columns": ':visible'
            },
        },
        {
            text: 'Excel',
            "extend": 'excelHtml5',
            "exportOptions": {
                "columns": ':visible'
            },
        },
        {
            text: 'CSV',
            "extend": 'csvHtml5',
            "exportOptions": {
                "columns": ':visible'
            },
        },
        {
            text: '自定义显示列',
            action: function (e, dt, node, config) {
                setTableColumnsData('dataTable2', tableColumns, selectedValues);
                $("#choicecolumns").modal();
            }
        }
        ],*/
        "columns": columnsArray,
        "data": tableData,
        "createdRow": function (row, data, dataIndex) {

            /*for (i = 0; i < tableColumns.length; i++) {
                if (tableColumns[i] == '股票代码' || tableColumns[i] == '股票名称') {
                    if (data[i].trim() != '') {
                        $(row).children('td').eq(i).addClass("Stock-Symbol");
                    }
                }
            
            }*/

        },
        "columnDefs": columnsDefArray,
        "initComplete": function () {
            tableInitComplate = 1;
            let api = this.api();
            api.$('.new_td').click(function () {
                let selectStockCode = '';
                let selectStockName = '';
                let tablehead = $("#dataTable4").find("thead").find("tr").eq(0).find("th");
                for (i = 0; i < tablehead.length; i++) {
                    let RowColumnText = tablehead.eq(i).text();
                    RowColumnText = RowColumnText.replace(/\s+/g, "");
                    if (RowColumnText == '股票代码') {
                        //console.log($(this).parent().find("td").eq(i));    
                        selectStockCode = $(this).parent().find("td").eq(i).text();
                    }
                    else if (RowColumnText == '股票名称') {
                        //console.log($(this).parent().find("td").eq(i));
                        selectStockName = $(this).parent().find("td").eq(i).text();
                    }

                }

                if (selectStockCode != '' && selectStockName != '') {
                    showStockDetail(selectStockCode, selectStockName);
                }
                else {
                    /*$("#InfoTitle").html('提示');
                    $("#InfoContent").html('股票代码获取失败，请将股票代码和股票名称列选择为可见。');
                    $("#Infomodal-1").modal();*/
                    toastr.error('股票代码获取失败，请将股票代码和股票名称列选择为可见。', "失败"); //失败提示框
                    return;
                }

            });
            // 动态添加的元素 生效
            $('body').tooltip({
                selector: '[data-toggle="tooltip"]'
            });

        }
    });
    //自动加下拖动条
    //如果表格不带有domstr，需要手动加上，则需要下面的代码才可以出现水平滚动条
    //$("#dataTable1").wrapAll("<div style='overflow-x:auto'>");

    //sleep(500);

    //动态实现表格列的显示隐藏
    //暂不需要，跳过
    /*
    $datatable = $('#'+tableName).dataTable();
    //hide datatable 1 all columns
    for (j = 0; j < $datatable.fnSettings().aoColumns.length; j++) {
        $datatable.fnSetColumnVis(j, false);
    }
    for (i = 0; i < selectedValues.length; i++) {
        $datatable.fnSetColumnVis(selectedValues[i], true);
    }
    */

    /*
    //双击行事件,以后再修改
    $('#dataTable1 tbody').on('dblclick', 'tr ', function (e) {
        var tdArr = $(this).children();
        //var fundcode = tdArr.eq(0).text();
        //var fundmanager = tdArr.eq(5).text();
        //console.log(fundcode ,fundmanager);
        editfundbaseinfo(tdArr)
    });*/
}

//显示任务列表  
function setTableData2(tableData, tableColumns) {
    //特殊设置
    //1、不显foot和head
    let domstr = '';

    //2、排序列,默认不排序
    let ordercolumns = [];

    //3、列名称
    let columnsArray = new Array();
    for (var idx in tableColumns) {
        /*if (in_array(tableColumns[idx],spField) == false) {
            columnsArray.push({ 'title': tableColumns[idx] + '&nbsp;&nbsp;', 'class': 'center' });
        }
        else
        {
            columnsArray.push({
                'title': tableColumns[idx] + '&nbsp;<i class="fa-question-circle big-fa" data-toggle="tooltip" data-placement="bottom" data-trigger="hover" \
                    data-container="body" data-original-title="' +fieldComment[tableColumns[idx]] +'"></i>&nbsp;&nbsp;&nbsp;&nbsp;', 'class': 'center' });
        }*/
        columnsArray.push({ 'title': tableColumns[idx] + '&nbsp;&nbsp;', 'class': 'center' });
    }

    //4、固定列
    let fixcolumns = { "leftColumns": 1, }; //固定左边第一列

    //5、列定义，包括格式化，隐藏，BAR显示等
    let columnsDefArray = new Array();
    let formatcolumns = [];
    let percentagecolumns = [];
    let nomarlformatcolumns = [];
    let hiddencolumns = [];
    let barColumns = [];
    let maxData = [];

    for (i = 0; i < tableColumns.length; i++) {
        if (tableColumns[i] == '暂不用') {
            //默认排序列
            //ordercolumns = [[i, 'desc']];
            barColumns.push(i);
            maxData.push(0);
        }
        else if (tableColumns[i] == '价格' || tableColumns[i] == 'PE') {
            formatcolumns.push(i);
        }
        else if (tableColumns[i].indexOf('变动') != -1) {
            percentagecolumns.push(i);
            //nomarlformatcolumns.push(i);
        }
        /*else if (in_array(tableColumns[i].indexOf('PE') != -1 || tableColumns[i].indexOf('PB') != -1 || tableColumns[i].indexOf('PS') != -1
            || tableColumns[i].indexOf('ROE') != -1)) {
            nomarlformatcolumns.push(i);
        }*/
    }

    columnsDefArray.push(
        {
            "targets": barColumns,
            "render": function (data, type, full, meta) {
                let data_index = meta.col;
                if (tableColumns[data_index] == '仓位(%)') {
                    let maxdataindex = in_arrayPos(data_index, barColumns);
                    return trBar(maxdataindex, maxData, data, '', 'left', 2, '#409ecc');
                }

                /*if (data_index == 1) {
                    
                }
                else if (in_array(data_index, maxDataArray1)) {

                    //return trBar(data_index, maxData, data, '', 'left', 2, '#409ecc');
                    return trBar(1, maxData, data, '', 'left', 2, '#8da0b6');
                }
                else if (in_array(data_index, maxDataArray2)) {
                    return trBar(2, maxData, data, '', 'center', 2);
                }*/

            }
        }
    );

    /*
    Orange
    Hex code: #FF9838
    RGB Value:  (255,152,56)

    Red
    Hex Code: #DC4E57
    RGB Value: (220,78,87)

    Yellow
    Hex code: #FDF4BF
    RGB Value: (255,244,191)

    Green
    Hex code: #C5E1A5
    RGB Value: (197,225,165)

    Blue
    Hex code: #4dcaf7
    Hex code2: #409ecc
    RGB Value: (77,202,247)
    */


    //百分比显示数据    
    columnsDefArray.push(
        {
            "targets": percentagecolumns,
            "render": function (data, type, full, meta) {
                return percentage(data, 2);
            }
        }
    );
    //格式化显示数据
    columnsDefArray.push(
        {
            "targets": formatcolumns,
            "render": function (data, type, full, meta) {
                let data_index = meta.col;
                if (tableColumns[data_index] == '持仓数量') {
                    return format(data, 0);
                }
                return format(data);
            }
        }
    );
    //格式化显示数据
    columnsDefArray.push(
        {
            "targets": nomarlformatcolumns,
            "render": function (data, type, full, meta) {
                return (data.toFixed(2));
            }
        }
    );
    columnsDefArray.push(
        {
            "targets": hiddencolumns,
            "visible": false
        }
    );




    let tableName = 'dataTable2';
    $('#' + tableName).dataTable().fnClearTable();   //将数据清除
    $('#' + tableName).dataTable().fnDestroy();   //将数据清除
    $('#' + tableName).empty();   //将数据清除  //清除列名,需要加载后台传入列名时需要
    $('#' + tableName).dataTable({
        //"sScrollX": "100%",
        //"bScrollCollapse": false,
        //"sScrollXInner": "100%",
        //"fixedColumns": fixcolumns, //要开始这个必须要有水平滚动条 和sScrollX联动
        "bAutoWidth": true,
        "autoWidth": true,
        "colReorder": true,  //可以水平拖动列  这个与柱状图有冲突，如果有柱状图，这个必须为false        
        "language": {
            "sProcessing": "处理中...",
            "sLengthMenu": "显示 _MENU_ 项结果",
            "sZeroRecords": "没有匹配结果",
            "sInfo": "显示第 _START_ 至 _END_ 项结果，共 _TOTAL_ 项",
            "sInfoEmpty": "显示第 0 至 0 项结果，共 0 项",
            "sInfoFiltered": "(由 _MAX_ 项结果过滤)",
            "sInfoPostFix": "",
            "sSearch": "",
            "searchPlaceholder": "搜索",
            "sUrl": "",
            "sEmptyTable": "表中数据为空",
            "sLoadingRecords": "载入中...",
            "sInfoThousands": ",",
            "oPaginate": {
                "sFirst": "首页",
                "sPrevious": "上页",
                "sNext": "下页",
                "sLast": "末页"
            },
            "oAria": {
                "sSortAscending": ": 以升序排列此列",
                "sSortDescending": ": 以降序排列此列"
            }
        },
        "destroy": true,
        "pageLength": 10,
        "bStateSave": false,   //不保存状态
        "dom": domstr,
        "order": ordercolumns,
        //"dom" :"lBfrtip",               
        "columns": columnsArray,
        "data": tableData,
        "createdRow": function (row, data, dataIndex) {

            /*for (i = 0; i < tableColumns.length; i++) {
                if (tableColumns[i] == '股票代码' || tableColumns[i] == '股票名称') {
                    if (data[i].trim() != '') {
                        $(row).children('td').eq(i).addClass("Stock-Symbol");
                    }
                }
            
            }*/

        },
        "columnDefs": columnsDefArray,
        "initComplete": function () {
            tableInitComplate = 1;
            let api = this.api();
            api.$('.new_td').click(function () {
                let selectStockCode = '';
                let selectStockName = '';
                let tablehead = $("#dataTable4").find("thead").find("tr").eq(0).find("th");
                for (i = 0; i < tablehead.length; i++) {
                    let RowColumnText = tablehead.eq(i).text();
                    RowColumnText = RowColumnText.replace(/\s+/g, "");
                    if (RowColumnText == '股票代码') {
                        //console.log($(this).parent().find("td").eq(i));    
                        selectStockCode = $(this).parent().find("td").eq(i).text();
                    }
                    else if (RowColumnText == '股票名称') {
                        //console.log($(this).parent().find("td").eq(i));
                        selectStockName = $(this).parent().find("td").eq(i).text();
                    }

                }

                if (selectStockCode != '' && selectStockName != '') {
                    showStockDetail(selectStockCode, selectStockName);
                }
                else {
                    /*$("#InfoTitle").html('提示');
                    $("#InfoContent").html('股票代码获取失败，请将股票代码和股票名称列选择为可见。');
                    $("#Infomodal-1").modal();*/
                    toastr.error('股票代码获取失败，请将股票代码和股票名称列选择为可见。', "失败"); //失败提示框
                    return;
                }

            });
            // 动态添加的元素 生效
            $('body').tooltip({
                selector: '[data-toggle="tooltip"]'
            });

        }
    });
    //自动加下拖动条
    //$("#dataTable1").wrapAll("<div style='overflow-x:auto'>");

    //sleep(500);

    //动态实现表格列的显示隐藏
    //暂不需要，跳过
    /*
    $datatable = $('#'+tableName).dataTable();
    //hide datatable 1 all columns
    for (j = 0; j < $datatable.fnSettings().aoColumns.length; j++) {
        $datatable.fnSetColumnVis(j, false);
    }
    for (i = 0; i < selectedValues.length; i++) {
        $datatable.fnSetColumnVis(selectedValues[i], true);
    }
    */

    /*
    //双击行事件,以后再修改
    $('#dataTable1 tbody').on('dblclick', 'tr ', function (e) {
        var tdArr = $(this).children();
        //var fundcode = tdArr.eq(0).text();
        //var fundmanager = tdArr.eq(5).text();
        //console.log(fundcode ,fundmanager);
        editfundbaseinfo(tdArr)
    });*/
}

//一个JQuery DataTable表格设置的例子
function setTableData4(tabledata, tableColumns) {
    console.log(tabledata);
    //从cookie中读取multi-select的选择
    let selectedValues = $.cookie('portfoliowatch_dataTable4_columnsdef');
    if (selectedValues == null) {
        selectedValues = [];
        for (j = 0; j < tableColumns.length; j++) {
            if (in_array(tableColumns[j], Table4DefShowColumns) == true) {
                selectedValues.push(j.toString());
            }
        }
    }
    else {
        selectedValues = selectedValues.split(',');
    }
    console.log(selectedValues);

    var columnsArray = new Array();
    var ordercolumns = new Array();
    var subColumnsArray = new Array();
    var fixcolumns = new Array();


    //ordercolumns = [[0, 'desc']];

    var percentagecolumns = [];
    var nomarlformatcolumns = [];
    var formatcolumns = [];
    var hiddencolumns = [];



    for (var idx in tableColumns) {
        /*if (in_array(tableColumns[idx],spField) == false) {
            columnsArray.push({ 'title': tableColumns[idx] + '&nbsp;&nbsp;', 'class': 'center' });
        }
        else
        {
            columnsArray.push({
                'title': tableColumns[idx] + '&nbsp;<i class="fa-question-circle big-fa" data-toggle="tooltip" data-placement="bottom" data-trigger="hover" \
                    data-container="body" data-original-title="' +fieldComment[tableColumns[idx]] +'"></i>&nbsp;&nbsp;&nbsp;&nbsp;', 'class': 'center' });
        }*/
        columnsArray.push({ 'title': tableColumns[idx] + '&nbsp;&nbsp;', 'class': 'center' });
    }

    /*for (i = 0; i < columnsArray.length - 2; i++) {
        columnsArray[i].title = "&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;" + columnsArray[i].title + "&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;";
        columnsArray[i].subtitle = "&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;" + columnsArray[i].subtitle + "&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;";
    }*/
    var domstr = "<'row'<'dataTables_header'<'col-md-3'B><'rightInfo'f>r>>t<'row'<'dataTables_footer clearfix'>>";
    if (tabledata.length > 10) {
        domstr = "<'row'<'dataTables_header'<'col-md-2'l><'col-md-3'B><'rightInfo'f>r>>t<'row'<'dataTables_footer clearfix'<'col-md-6'i><'col-md-6'p>>>";
    }
    //需要按柱状图显示的列 （暂不需要）    
    /*var maxData = [0];
    var maxCol = [7]
    for (i = 0; i < tabledata.length; i++) {
        for (j = 0; j < maxCol.length; j++) {    
            if (maxData[j] < tabledata[i][maxCol[j]]) {
                maxData[j] = tabledata[i][maxCol[j]];
            }
        }
    }*/

    let columnsDefArray = new Array();
    let barColumns = [];
    let maxData = [];
    let maxDataGroupValue = 0;
    //需要按柱状图显示的列
    for (i = 0; i < tableColumns.length; i++) {
        if (tableColumns[i] == '仓位(%)') {
            //默认排序列
            //ordercolumns = [[i, 'desc']];
            barColumns.push(i);
            maxData.push(0);
        }
        else if (tableColumns[i] == '持仓数量' || tableColumns[i] == '持仓市值(万元)' || tableColumns[i] == '总市值(亿)') {
            formatcolumns.push(i);
        }
        else if (tableColumns[i].indexOf('分位数') != -1) {
            //percentagecolumns.push(i);
            nomarlformatcolumns.push(i);
        }
        else if (in_array(tableColumns[i].indexOf('PE') != -1 || tableColumns[i].indexOf('PB') != -1 || tableColumns[i].indexOf('PS') != -1
            || tableColumns[i].indexOf('ROE') != -1)) {
            nomarlformatcolumns.push(i);
        }
    }

    for (i = 0; i < tabledata.length; i++) {
        for (j = 0; j < barColumns.length; j++) {
            if (maxData[j] < Math.abs(tabledata[i][barColumns[j]])) {
                maxData[j] = Math.abs(tabledata[i][barColumns[j]]);
            }
        }
    }

    columnsDefArray.push(
        {
            "targets": barColumns,
            "render": function (data, type, full, meta) {
                let data_index = meta.col;
                if (tableColumns[data_index] == '仓位(%)') {
                    let maxdataindex = in_arrayPos(data_index, barColumns);
                    return trBar(maxdataindex, maxData, data, '', 'left', 2, '#409ecc');
                }

                /*if (data_index == 1) {
                    
                }
                else if (in_array(data_index, maxDataArray1)) {

                    //return trBar(data_index, maxData, data, '', 'left', 2, '#409ecc');
                    return trBar(1, maxData, data, '', 'left', 2, '#8da0b6');
                }
                else if (in_array(data_index, maxDataArray2)) {
                    return trBar(2, maxData, data, '', 'center', 2);
                }*/

            }
        }
    );

    /*
    Orange
    Hex code: #FF9838
    RGB Value:  (255,152,56)

    Red
    Hex Code: #DC4E57
    RGB Value: (220,78,87)

    Yellow
    Hex code: #FDF4BF
    RGB Value: (255,244,191)

    Green
    Hex code: #C5E1A5
    RGB Value: (197,225,165)

    Blue
    Hex code: #4dcaf7
    Hex code2: #409ecc
    RGB Value: (77,202,247)
    */


    //百分比显示数据    
    columnsDefArray.push(
        {
            "targets": percentagecolumns,
            "render": function (data, type, full, meta) {
                return percentage(data, 2);
            }
        }
    );
    //格式化显示数据
    columnsDefArray.push(
        {
            "targets": formatcolumns,
            "render": function (data, type, full, meta) {
                let data_index = meta.col;
                if (tableColumns[data_index] == '持仓数量') {
                    return format(data, 0);
                }
                return format(data);
            }
        }
    );
    //格式化显示数据
    columnsDefArray.push(
        {
            "targets": nomarlformatcolumns,
            "render": function (data, type, full, meta) {
                return (data.toFixed(2));
            }
        }
    );
    columnsDefArray.push(
        {
            "targets": hiddencolumns,
            "visible": false
        }
    );





    fixcolumns = { "leftColumns": 4, }; //固定左边五列         



    $('#dataTable4').dataTable().fnClearTable();   //将数据清除
    $('#dataTable4').dataTable().fnDestroy();   //将数据清除
    $('#dataTable4').empty();   //将数据清除  //清除列名,需要加载后台传入列名时需要
    $('#dataTable4').dataTable({
        "sScrollX": "100 % ",
        "bScrollCollapse": false,
        "sScrollXInner": "100%",
        "bAutoWidth": true,
        "autoWidth": true,
        "colReorder": false,
        "fixedColumns": fixcolumns,
        "language": {
            "sProcessing": "处理中...",
            "sLengthMenu": "显示 _MENU_ 项结果",
            "sZeroRecords": "没有匹配结果",
            "sInfo": "显示第 _START_ 至 _END_ 项结果，共 _TOTAL_ 项",
            "sInfoEmpty": "显示第 0 至 0 项结果，共 0 项",
            "sInfoFiltered": "(由 _MAX_ 项结果过滤)",
            "sInfoPostFix": "",
            "sSearch": "",
            "searchPlaceholder": "搜索",
            "sUrl": "",
            "sEmptyTable": "表中数据为空",
            "sLoadingRecords": "载入中...",
            "sInfoThousands": ",",
            "oPaginate": {
                "sFirst": "首页",
                "sPrevious": "上页",
                "sNext": "下页",
                "sLast": "末页"
            },
            "oAria": {
                "sSortAscending": ": 以升序排列此列",
                "sSortDescending": ": 以降序排列此列"
            }
        },
        "destroy": true,
        "pageLength": 10,
        "bStateSave": false,
        "dom": domstr,
        "order": ordercolumns,
        //"dom" :"lBfrtip",
        "buttons": [{
            text: 'Copy',
            "extend": 'copyHtml5',
            "exportOptions": {
                "columns": ':visible'
            },
        },
        {
            text: 'Excel',
            "extend": 'excelHtml5',
            "exportOptions": {
                "columns": ':visible'
            },
        },
        {
            text: 'CSV',
            "extend": 'csvHtml5',
            "exportOptions": {
                "columns": ':visible'
            },
        },
        {
            text: '自定义显示列',
            action: function (e, dt, node, config) {
                setTableColumnsData('dataTable1', tableColumns, selectedValues);
                $("#choicecolumns").modal();
            }
        }
        ],
        "columns": columnsArray,
        "data": tabledata,
        "createdRow": function (row, data, dataIndex) {

            for (i = 0; i < tableColumns.length; i++) {
                if (tableColumns[i] == '股票代码' || tableColumns[i] == '股票名称') {
                    if (data[i].trim() != '') {
                        $(row).children('td').eq(i).addClass("new_td");
                    }
                }
                else if (tableColumns[i] == 'PE') {
                    if (data[i] > 40) {
                        $(row).children('td').eq(i).addClass("ctdlv1");
                    }
                }
                else if (tableColumns[i] == 'PE分位数') {
                    if (data[i] > 80) {
                        $(row).children('td').eq(i).addClass("ctdlv1");
                    }

                }
            }

        },
        "columnDefs": columnsDefArray,
        "initComplete": function () {
            tableInitComplate = 1;
            let api = this.api();
            api.$('.new_td').click(function () {
                let selectStockCode = '';
                let selectStockName = '';
                let tablehead = $("#dataTable4").find("thead").find("tr").eq(0).find("th");
                for (i = 0; i < tablehead.length; i++) {
                    let RowColumnText = tablehead.eq(i).text();
                    RowColumnText = RowColumnText.replace(/\s+/g, "");
                    if (RowColumnText == '股票代码') {
                        //console.log($(this).parent().find("td").eq(i));    
                        selectStockCode = $(this).parent().find("td").eq(i).text();
                    }
                    else if (RowColumnText == '股票名称') {
                        //console.log($(this).parent().find("td").eq(i));
                        selectStockName = $(this).parent().find("td").eq(i).text();
                    }

                }

                if (selectStockCode != '' && selectStockName != '') {
                    showStockDetail(selectStockCode, selectStockName);
                }
                else {
                    $("#InfoTitle").html('提示');
                    $("#InfoContent").html('股票代码获取失败，请将股票代码和股票名称列选择为可见。');
                    $("#Infomodal-1").modal();
                    return;
                }

            });
            // 动态添加的元素 生效
            $('body').tooltip({
                selector: '[data-toggle="tooltip"]'
            });

        }
    });

    //$("#dataTable1").wrapAll("<div style='overflow-x:auto'>");

    //sleep(500);


    $datatable4 = $('#dataTable4').dataTable();
    //hide datatable 1 all columns
    for (j = 0; j < $datatable4.fnSettings().aoColumns.length; j++) {
        $datatable4.fnSetColumnVis(j, false);
    }
    for (i = 0; i < selectedValues.length; i++) {
        $datatable4.fnSetColumnVis(selectedValues[i], true);
    }

    /*
    $('#dataTable1 tbody').on('dblclick', 'tr ', function (e) {
        var tdArr = $(this).children();
        //var fundcode = tdArr.eq(0).text();
        //var fundmanager = tdArr.eq(5).text();
        //console.log(fundcode ,fundmanager);
        editfundbaseinfo(tdArr)
    });*/
}
