<?php

use Illuminate\Support\Facades\Route;

/*
|--------------------------------------------------------------------------
| Web Routes
|--------------------------------------------------------------------------
|
| Here is where you can register web routes for your application. These
| routes are loaded by the RouteServiceProvider within a group which
| contains the "web" middleware group. Now create something great!
|
*/

Route::get('/', function () {
    return view('welcome');
});

//use App\KafkaController;
#Route::get('/kafka', [KafkaController::class, 'index']);

Route::get('/kafka', 'App\Http\Controllers\KafkaController@index');
Route::get('/consume', 'App\Http\Controllers\KafkaController@consume');
Route::get('/my-route', 'App\Http\Controllers\MyController@index');
