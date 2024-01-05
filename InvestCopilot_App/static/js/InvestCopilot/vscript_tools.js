/******************************/
/*      通用函数代码           */
/*        JS V0.0.1           */
/*     Create by  Rocky       */
/*      in 20231003           */
/******************************/
//var exptableData = '1';
if (!String.prototype.startsWith) {
    (function () {
        'use strict'; // needed to support `apply`/`call` with `undefined`/`null`
        var defineProperty = (function () {
            // IE 8 only supports `Object.defineProperty` on DOM elements
            try {
                var object = {};
                var $defineProperty = Object.defineProperty;
                var result = $defineProperty(object, object, object) && $defineProperty;
            } catch (error) { }
            return result;
        }());
        var toString = {}.toString;
        var startsWith = function (search) {
            if (this == null) {
                throw TypeError();
            }
            var string = String(this);
            if (search && toString.call(search) == '[object RegExp]') {
                throw TypeError();
            }
            var stringLength = string.length;
            var searchString = String(search);
            var searchLength = searchString.length;
            var position = arguments.length > 1 ? arguments[1] : undefined;
            // `ToInteger`
            var pos = position ? Number(position) : 0;
            if (pos != pos) { // better `isNaN`
                pos = 0;
            }
            var start = Math.min(Math.max(pos, 0), stringLength);
            // Avoid the `indexOf` call if no match is possible
            if (searchLength + start > stringLength) {
                return false;
            }
            var index = -1;
            while (++index < searchLength) {
                if (string.charCodeAt(start + index) != searchString.charCodeAt(index)) {
                    return false;
                }
            }
            return true;
        };
        if (defineProperty) {
            defineProperty(String.prototype, 'startsWith', {
                'value': startsWith,
                'configurable': true,
                'writable': true
            });
        } else {
            String.prototype.startsWith = startsWith;
        }
    }());
}

function isNull(value) {
    if (!value && typeof value != "undefined" && value != 0) {
        return true;
    } else if (value.length == 0) {
        return true;
    }
    else {
        return false;
    }
}

function colorRGBtoHex(color) {
    var rgb = color.split(',');
    var r = parseInt(rgb[0].split('(')[1]);
    var g = parseInt(rgb[1]);
    var b = parseInt(rgb[2].split(')')[0]);
    var hex = "#" + ((1 << 24) + (r << 16) + (g << 8) + b).toString(16).slice(1);
    return hex;
}


function dealyRun(code, time) {
    var t = setTimeout(code, time);
}
function in_array(search, array) {
    for (let i in array) {
        if (array[i] == search) {
            return true;
        }
    }
    return false;
}
function in_arrayPos(search, array) {
    for (let i in array) {
        if (array[i] == search) {
            return i;
        }
    }
    return -1;
}

function format(n, fixnum = 2) {
    n = n.toFixed(fixnum)
    let num = n.toString()
    n1 = Math.abs(n)
    n1 = n1.toFixed(fixnum)
    let num1 = n1.toString()
    let decimals = ''
    // 判断是否有小数
    if (fixnum > 0) {
        num.indexOf('.') > -1 ? decimals = num.split('.')[1] : decimals
    }
    let len = num.length;
    if (fixnum > 0) {
        len = num.split('.')[0].length
    }


    if (len <= 3) {
        return num
    } else {
        if (num.length == num1.length) {
            let temp = ''
            let remainder = len % 3
            decimals ? temp = '.' + decimals : temp
            if (remainder > 0) { // 不是3的整数倍
                return num.slice(0, remainder) + ',' + num.slice(remainder, len).match(/\d{3}/g).join(',') + temp
            } else { // 是3的整数倍
                return num.slice(0, len).match(/\d{3}/g).join(',') + temp
            }
        }
        else {
            num = num1
            let len = num.split('.')[0].length
            let temp = ''
            let remainder = len % 3
            decimals ? temp = '.' + decimals : temp
            if (remainder > 0) { // 不是3的整数倍
                return '-' + num.slice(0, remainder) + ',' + num.slice(remainder, len).match(/\d{3}/g).join(',') + temp
            } else { // 是3的整数倍
                return '-' + num.slice(0, len).match(/\d{3}/g).join(',') + temp
            }
        }

    }
}
function percentage(n, fixnum) {
    var rate = Math.round(n * 10000) / 100;
    return rate.toFixed(fixnum) + '%';
}
function trBar(data_index, maxData, data, mode, align, fixnum, color, color2) {
    /*maxData数组是所有要显示的值的各列中最大值，data_index为要显示的第几个（这个是相对maxData来的),data是数值，mode为百分化或者不百分化，
        align表示向左靠（全红正数显示效果）还是居中（有正负数的情况），fixnum是小数点后显示几位）*/
    //var leftBarColor ='#E45353';
    var leftBarColor = color;
    var rightBarColor = color2;
    //定制BARCOLOR 
    //leftBarColor = '#84c1ff';
    var barHeight = '1.8';
    if (data != -100) {
        if (mode == "%") {
            var rate = Math.round(data * 10000) / 100;
            var _height = Math.abs(data) * 100 / maxData[data_index];
            var s_v = rate.toFixed(fixnum) + "%";
        }
        else if (mode == "bp") {
            var rate = Math.round(data * 1000000) / 100;
            var _height = Math.abs(data) * 100 / maxData[data_index];
            var s_v = rate.toFixed(fixnum) + "bp";
        }
        else if (mode == 'cash') {
            //var rate = Math.round(data * 1000000) / 100;            
            var _height = Math.abs(data) * 100 / maxData[data_index];
            var s_v = format(data, fixnum);
        }
        else {
            // var rate = Math.round(data);
            var _height = Math.abs(data) * 100 / maxData[data_index];
            var s_v = data.toFixed(fixnum) + '';
        }
        if (align == "left") {
            var td = '<div class="progress" style="width:100%;height:' + barHeight + 'rem;line-height:' + barHeight + 'rem;float:right;position:relative;">\n' +
                '<div class="progress-bar" role="progressbar" aria-valuenow="60" aria-valuemin="0" aria-valuemax="100"' +
                ' style="width:' + _height + '%;background-color:' + leftBarColor + ';"></div>\n' +
                '<span style="position:absolute;left:0;top:0;transform:rotate(0deg);' +
                'border-left-width:1px;border-left-color:black;border-left-style: solid;">' + s_v + '</span>\n' +
                '</div></span>';//;border-left: 2px dashed #350000
            return td;
        }
        else if (align == "center2") {
            if (data < 0) {
                var td = '<div class="progress" style="width:50%;height:' + barHeight + 'rem;line-height:' + barHeight + 'rem;float:left;transform: rotate(180deg);position:relative;">' +
                    '<div class="progress-bar" role="progressbar" aria-valuenow="60" aria-valuemin="0" aria-valuemax="100"' +
                    ' style="width:' + _height + '%;background-color:' + rightBarColor + ';">' +
                    '</div><span style="position:absolute;left:0;top:0;transform: rotate(180deg);' +
                    'border-right-width:1px;border-right-color:black;border-right-style: solid;">' + s_v + '</span>' +
                    '</div>';
                return td;
            }
            if (data >= 0) {
                var td = '<div class="progress" style="width:50%;height:' + barHeight + 'rem;line-height:' + barHeight + 'rem;float:right;position:relative;">\n' +
                    '<div class="progress-bar" role="progressbar" aria-valuenow="60" aria-valuemin="0" aria-valuemax="100"' +
                    ' style="width:' + _height + '%;background-color:' + leftBarColor + ';"></div>\n' +
                    '<span style="position:absolute;left:0;top:0;transform:rotate(0deg);' +
                    'border-left-width:1px;border-left-color:black;border-left-style: solid;">' + s_v + '</span>\n' +
                    '</div></span>';//;border-left: 2px dashed #350000
                return td;
            }

        }
        else if (align == "center") {
            if (data < 0) {
                var td = '<div class="progress" style="width:50%;height:' + barHeight + 'rem;line-height:' + barHeight + 'rem;float:left;transform: rotate(180deg);position:relative;">' +
                    '<div class="progress-bar" role="progressbar" aria-valuenow="60" aria-valuemin="0" aria-valuemax="100"' +
                    ' style="width:' + _height + '%;background-color:#00C484;">' +
                    '</div><span style="position:absolute;left:0;top:0;transform: rotate(180deg);' +
                    'border-right-width:1px;border-right-color:black;border-right-style: solid;">' + s_v + '</span>' +
                    '</div>';
                return td;
            }
            if (data >= 0) {
                var td = '<div class="progress" style="width:50%;height:' + barHeight + 'rem;line-height:' + barHeight + 'rem;float:right;position:relative;">\n' +
                    '<div class="progress-bar" role="progressbar" aria-valuenow="60" aria-valuemin="0" aria-valuemax="100"' +
                    ' style="width:' + _height + '%;background-color:#E45353;"></div>\n' +
                    '<span style="position:absolute;left:0;top:0;transform:rotate(0deg);' +
                    'border-left-width:1px;border-left-color:black;border-left-style: solid;">' + s_v + '</span>\n' +
                    '</div></span>';//;border-left: 2px dashed #350000
                return td;
            }
        }
        return s_v;
    }
    else {
        return '';
    }
}

function sleep(delay) {
    let start = (new Date()).getTime();
    while ((new Date()).getTime() - start < delay) {
        continue;
    }
}

function getToolBoxSet(chartdata, chartcolumns) {
    let toolboxset = {
        show: false
    };
    let toolsboxcontent = {
        show: true,
        feature: {
            dataView: {
                show: true,
                title: '数据视图',
                readOnly: true,
                optionToContent: function (opt) {
                    let axisLabel = chartcolumns;
                    let axisData = chartdata;
                    let table = '<table class="new_table table table-bordered   table-responsive  table-striped"><tbody>'
                    table += '<tr>';
                    for (let i = 0, l = axisLabel.length; i < l; i++) {
                        table += '<td>' + axisLabel[i] + '</td>'
                    }
                    table = table + '</tr>';

                    for (let i = 0, l = axisData.length; i < l; i++) {
                        table += '<tr>'
                        for (let j = 0; j < axisData[i].length; j++) {
                            table = table + '<td>' + axisData[i][j] + '</td>'
                        }
                        table = table + '</tr>';
                    }
                    table = table + '</tbody></table>';
                    return table;
                }
            },
            myTools: {
                show: true,
                title: '导出',
                icon: 'path://M745 184.3V1H93v1022.5h836V184.3zM500.8 476.2l76.6-131h67.7L532.5 537.9 445.7 686H378l122.8-209.8z m-0.7 70.3l-6.6-11-112.7-190.3h67.7L525 474.4l8.9 15.2L650.3 686h-67.7l-82.5-139.5z',
                onclick: function (view) {

                    let sourceData = [];
                    let axisLabel = chartcolumns;
                    let axisData = chartdata;

                    sourceData.push(axisLabel);

                    for (let i = 0, l = axisData.length; i < l; i++) {


                        sourceData.push(axisData[i]);

                    }
                    //console.log(sourceData);
                    let sheet = data2sheet(sourceData);
                    let blob = sheet2blob(sheet);
                    openDownloadDialog(blob, '图表数据导出.xlsx');
                }
            },
            saveAsImage: {}
        }
    };
    if (exptableData == '1') {
        toolboxset = toolsboxcontent;
    }
    return toolboxset;
}

/**************************************/
/**   EXCEL Proc Function            **/
/**************************************/
// 读取本地excel文件
function readWorkbookFromLocalFile(file, callback) {
    var reader = new FileReader();
    reader.onload = function (e) {
        var data = e.target.result;
        var workbook = XLSX.read(data, { type: 'binary' });
        if (callback) callback(workbook);
    };
    reader.readAsBinaryString(file);
}

// 从网络上读取某个excel文件，url必须同域，否则报错
function readWorkbookFromRemoteFile(url, callback) {
    var xhr = new XMLHttpRequest();
    xhr.open('get', url, true);
    xhr.responseType = 'arraybuffer';
    xhr.onload = function (e) {
        if (xhr.status == 200) {
            var data = new Uint8Array(xhr.response)
            var workbook = XLSX.read(data, { type: 'array' });
            if (callback) callback(workbook);
        }
    };
    xhr.send();
}

// 读取 excel文件
function outputWorkbook(workbook) {
    var sheetNames = workbook.SheetNames; // 工作表名称集合
    sheetNames.forEach(name => {
        var worksheet = workbook.Sheets[name]; // 只能通过工作表名称来获取指定工作表
        for (var key in worksheet) {
            // v是读取单元格的原始值
            console.log(key, key[0] === '!' ? worksheet[key] : worksheet[key].v);
        }
    });
}

function readWorkbook(workbook) {
    var sheetNames = workbook.SheetNames; // 工作表名称集合
    var worksheet = workbook.Sheets[sheetNames[0]]; // 这里我们只读取第一张sheet
    var csv = XLSX.utils.sheet_to_csv(worksheet);
    document.getElementById('result').innerHTML = csv2table(csv);
}

// 将csv转换成表格
function csv2table(csv) {
    var html = '<table>';
    var rows = csv.split('\n');
    rows.pop(); // 最后一行没用的
    rows.forEach(function (row, idx) {
        var columns = row.split(',');
        columns.unshift(idx + 1); // 添加行索引
        if (idx == 0) { // 添加列索引
            html += '<tr>';
            for (var i = 0; i < columns.length; i++) {
                html += '<th>' + (i == 0 ? '' : String.fromCharCode(65 + i - 1)) + '</th>';
            }
            html += '</tr>';
        }
        html += '<tr>';
        columns.forEach(function (column) {
            html += '<td>' + column + '</td>';
        });
        html += '</tr>';
    });
    html += '</table>';
    return html;
}

function table2csv(table) {
    var csv = [];
    $(table).find('tr').each(function () {
        var temp = [];
        $(this).find('td').each(function () {
            temp.push($(this).html());
        })
        temp.shift(); // 移除第一个
        csv.push(temp.join(','));
    });
    csv.shift();
    return csv.join('\n');
}

// csv转sheet对象
function csv2sheet(csv) {
    var sheet = {}; // 将要生成的sheet
    csv = csv.split('\n');
    csv.forEach(function (row, i) {
        row = row.split(',');
        if (i == 0) sheet['!ref'] = 'A1:' + String.fromCharCode(65 + row.length - 1) + (csv.length - 1);
        row.forEach(function (col, j) {
            sheet[String.fromCharCode(65 + j) + (i + 1)] = { v: col };
        });
    });
    return sheet;
}

function colnum2Char(num) {
    var colChar = '';
    if (num > 26) {
        colChar = String.fromCharCode(65 + Math.floor(num / 26) - 1) + String.fromCharCode(65 + num % 26 - 1);
    }
    else {
        colChar = String.fromCharCode(65 + num - 1)
    }
    return colChar;
}
// Array转sheet对象
function data2sheet(data) {
    var sheet = {}; // 将要生成的sheet

    sheet['!ref'] = 'A1:' + colnum2Char(data[0].length) + (data.length);

    for (i = 0; i < data.length; i++) {
        for (j = 0; j < data[i].length; j++) {
            sheet[colnum2Char(j + 1) + (i + 1)] = { v: data[i][j] };
        }
    }
    console.log(sheet);
    return sheet;
}

// 将一个sheet转成最终的excel文件的blob对象，然后利用URL.createObjectURL下载
function sheet2blob(sheet, sheetName) {
    sheetName = sheetName || 'sheet1';
    var workbook = {
        SheetNames: [sheetName],
        Sheets: {}
    };
    workbook.Sheets[sheetName] = sheet;
    // 生成excel的配置项
    var wopts = {
        bookType: 'xlsx', // 要生成的文件类型
        bookSST: false, // 是否生成Shared String Table，官方解释是，如果开启生成速度会下降，但在低版本IOS设备上有更好的兼容性
        type: 'binary'
    };
    var wbout = XLSX.write(workbook, wopts);
    var blob = new Blob([s2ab(wbout)], { type: "application/octet-stream" });
    // 字符串转ArrayBuffer
    function s2ab(s) {
        var buf = new ArrayBuffer(s.length);
        var view = new Uint8Array(buf);
        for (var i = 0; i != s.length; ++i) view[i] = s.charCodeAt(i) & 0xFF;
        return buf;
    }
    return blob;
}

/**
 * 通用的打开下载对话框方法，没有测试过具体兼容性
 * @param url 下载地址，也可以是一个blob对象，必选
 * @param saveName 保存文件名，可选
 */
function openDownloadDialog(url, saveName) {
    if (typeof url == 'object' && url instanceof Blob) {
        url = URL.createObjectURL(url); // 创建blob地址
    }
    var aLink = document.createElement('a');
    aLink.href = url;
    aLink.download = saveName || ''; // HTML5新增的属性，指定保存文件名，可以不要后缀，注意，file:///模式下不会生效
    var event;
    if (window.MouseEvent) event = new MouseEvent('click');
    else {
        event = document.createEvent('MouseEvents');
        event.initMouseEvent('click', true, false, window, 0, 0, 0, 0, 0, false, false, false, false, 0, null);
    }
    aLink.dispatchEvent(event);
}
