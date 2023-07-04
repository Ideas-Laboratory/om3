const createError = require('http-errors');
const express = require('express');
const path = require('path');
const cookieParser = require('cookie-parser');
const logger = require('morgan');
const bodyParser = require('body-parser')
const cors=require('cors')


//const linechartRouter=require('./routes/linechart');
const postgresLineChartRouter=require('./routes/linechart_postgres');
const fileRouter=require('./routes/file_download')

const app = express();
app.use(cors())

// view engine setup
app.set('views', path.join(__dirname, 'views'));
app.set('view engine', 'jade');

app.use(cookieParser())
app.use(bodyParser.urlencoded({ extended: false }))


app.use(bodyParser.json({limit:"10MB"}))

app.use(express.static(path.join(__dirname, 'public')));
app.use(express.static(path.join(__dirname, 'public')));
app.use(express.static(path.join(__dirname, 'public/dist')));
app.use(express.static(path.join(__dirname, 'public/dist/css')));
app.use(express.static(path.join(__dirname, 'public/dist/js')));
app.use(express.static(path.join(__dirname,'views')));


app.use('/postgres/line_chart',postgresLineChartRouter);
app.use('/file',fileRouter)
//app.use('/influx/line_chart',influxRouter);

// catch 404 and forward to error handler
app.use(function(req, res, next) {
  next(createError(404));
});

// error handler
app.use(function(err, req, res, next) {
  // set locals, only providing error in development
  res.locals.message = err.message;
  res.locals.error = req.app.get('env') === 'development' ? err : {};

  // render the error page
  res.status(err.status || 500);
  res.render('error');
});

module.exports = app;
