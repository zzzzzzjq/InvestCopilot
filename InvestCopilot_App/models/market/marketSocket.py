# from flask import Flask, render_template
# # from flask_socketio import SocketIO, emit, join_room, leave_room
# from flask_socketio import SocketIO, emit, join_room, leave_room, disconnect
#
# #pip3.10 install Flask Flask-SocketIO
# #pip3.10 install gunicorn
# import time
# import threading
# import random
#
# app = Flask(__name__)
# socketio = SocketIO(app, cors_allowed_origins='*')
#
# # 存储股票价格的字典
# stock_prices = {}
#
# # 模拟股票价格
# def simulate_stock_prices():
#     while True:
#         print("xxx")
#         time.sleep(1)
#         for stock_symbol in stock_prices:
#             stock_price = round(random.uniform(100, 200), 2)
#             stock_prices[stock_symbol] = stock_price
#             print("stock_symbol:%s,stock_price:%s"%(stock_symbol,stock_price))
#             socketio.emit('stock_price_update', {'symbol': stock_symbol, 'price': stock_price}, namespace='/stock', room=stock_symbol)
#
# # 启动模拟
# threading.Thread(target=simulate_stock_prices).start()
#
# @app.route('/')
# def index():
#     return render_template('index.html')
#
# @socketio.on('subscribe', namespace='/stock')
# def handle_subscribe(data):
#     print("handle_subscribe data:",data)
#     symbol = data['symbol']
#     print("handle_subscribe symbol:",symbol)
#     room = symbol.upper()  # Convert stock symbol to uppercase as room name
#     if room not in stock_prices:
#         stock_prices[room] = 0  # Initialize stock price
#     join_room(room)
#     emit('subscribed', {'symbol': symbol})
#
# @socketio.on('unsubscribe', namespace='/stock')
# def handle_unsubscribe(data):
#     print("handle_unsubscribe data:",data)
#     symbol = data['symbol']
#     print("handle_unsubscribe symbol:",symbol)
#     room = symbol.upper()
#     leave_room(room)
#     emit('unsubscribed', {'symbol': symbol})
#
# @socketio.on('disconnect')
# def handle_disconnect():
#     # 在断开连接时取消订阅
#     print(f"Client disconnected: {request.sid}")
#     disconnect()
#
#
# if __name__ == '__main__':
#     # socketio.run(app)
#     socketio.run(app, allow_unsafe_werkzeug=True, debug=True)
"""
<script src="https://cdnjs.cloudflare.com/ajax/libs/socket.io/4.1.2/socket.io.js"></script>

 <script>
        const socket = io('http://127.0.0.1:5000/stock');

        socket.on('connect', function() {
            console.log('Connected to server');
        });

        socket.on('disconnect', function() {
            console.log('Disconnected from server');
        });

        socket.on('subscribed', function(data) {
            console.log(`Subscribed to ${data.symbol}`);
        });

        socket.on('unsubscribed', function(data) {
            console.log(`Unsubscribed from ${data.symbol}`);
        });

        socket.on('stock_price_update', function(data) {
            console.log("data:",data)
            {#const stockPriceDiv = document.getElementById(`stockPrice_${data.symbol}`);#}
            {##}
            {#if (stockPriceDiv) {#}
            {#    stockPriceDiv.innerText = `${data.symbol}: $${data.price}`;#}
            {#} else {#}
            {#    const newStockPriceDiv = document.createElement('div');#}
            {#    newStockPriceDiv.id = `stockPrice_${data.symbol}`;#}
            {#    newStockPriceDiv.innerText = `${data.symbol}: $${data.price}`;#}
            {#    document.getElementById('stockPrices').appendChild(newStockPriceDiv);#}
            {#}#}
        });

        function subscribeToStock(symbol) {
            console.log("symbol:",symbol)
            socket.emit('subscribe', { symbol });
        }

        function unsubscribeFromStock(symbol) {
            socket.emit('unsubscribe', { symbol });
            const stockPriceDiv = document.getElementById(`stockPrice_${symbol}`);
            if (stockPriceDiv) {
                stockPriceDiv.remove();
            }
        }

        const initialStocks = ['AAPL', 'GOOGL', 'MSFT', 'AMZN'];
        initialStocks.forEach((symbol) => subscribeToStock(symbol));


// 在页面关闭前触发取消订阅
window.addEventListener('beforeunload', () => {
    // 在这里添加取消订阅的逻辑
    const username = 'your_username';  // 替换为实际的用户名
    const subscribedStocks = ['AAPL', 'GOOGL', 'MSFT', 'AMZN'];  // 替换为实际已订阅的股票
    subscribedStocks.forEach((symbol) => unsubscribeFromStock(symbol, username));
});
    </script>


"""
pass