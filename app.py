import os
import uuid

import matplotlib
import shutil
from flask import Flask, render_template, request, flash, redirect, url_for

import database_manager as dm

matplotlib.use('Agg')

import matplotlib.pyplot as plt

app = Flask(__name__)
app.config['IMG_FOLDER'] = 'static/images'


@app.route('/')
def index():
    return render_template('index.html', datasets=dm.get_data_sets())


@app.route('/init_database')
def init_database():
    dm.init_database()
    if os.path.exists('static/images'):
        shutil.rmtree('static/images')
        os.mkdir('static/images')
    return "<h1>Success to init database</h1>"


@app.route('/upload_csv', methods=['GET', 'POST'])
def upload_csv():
    if request.method == 'GET':
        return render_template('upload.html')
    else:
        if 'csv_file' not in request.files:
            app.logger.debug("NO File part")
            flash('No file part')
            return redirect(url_for('upload_csv'))
        csv_file = request.files['csv_file']
        if csv_file.filename == '':
            flash('No selected file!')
            return redirect(url_for('upload_csv'))
        if csv_file.filename.rsplit('.', 1)[1].lower() != 'csv':
            flash('Uploaded file should be csv type')
            return redirect(url_for('upload_csv'))
        data_set_name = request.form['DataSetName'].strip()
        data_set_name.replace(' ', '_')
        if data_set_name == '':
            flash('Data set name is Empty!')
            return redirect(url_for('upload_csv'))
        if dm.exist_data_set(data_set_name):
            flash('{} is exists!'.format(data_set_name))
            return redirect(url_for('upload_csv'))
        app.logger.debug(data_set_name)
        info = dm.add_dataset(data_set_name, csv_file)
        app.logger.debug(info.describe())
        return redirect(url_for('index'))


@app.route('/del_dataset/<name>')
def delete_data_set(name):
    dm.delete_data_set(name)
    return redirect(url_for('index'))


@app.route('/dataset/<name>')
def view_data_set(name):
    return render_template('dataset.html', table_name=name, col_infos=dm.get_col_infos(name))


@app.route('/column/<table_name>/<col_name>')
def view_column(table_name, col_name):
    param_dict = {
        'table_name': table_name,
        'col_name': col_name,
        'col_info': dm.get_col_info(table_name, col_name),
        'col_infos': dm.get_col_infos(table_name),
        'images': dm.get_all_images(table_name, col_name),
        'target_col_info': dm.get_col_info(table_name+'_TARGET', col_name),
    }
    return render_template('column.html', **param_dict)


@app.route('/add_figure', methods=['POST'])
def add_figure():
    fig_folder = app.config['IMG_FOLDER']
    fig_name = str(uuid.uuid4())
    table_name = request.form['table_name']
    col_name = request.form['col_name']
    col_values = dm.get_col_values(table_name, col_name)
    fig_title = request.form['fig_title']
    fig_param = request.form['fig_param']
    fig_args = []
    fig_kwargs = {}
    for s in fig_param.split(','):
        s = s.strip()
        if s:
            pairs = s.strip().split('=')
            if len(pairs) > 1:
                key = pairs[0]
                try:
                    value = float(pairs[1])
                except ValueError:
                    value = pairs[1]
                fig_kwargs[key] = value
            else:
                fig_args.append(pairs[0])
    plt.figure()
    if request.form['fig_type'] == 'hist':
        plt.hist(col_values, *fig_args, **fig_kwargs)
    else:
        rel_col_name = request.form['rel_column']
        rel_col_values = dm.get_col_values(table_name, rel_col_name)
        if fig_args:
            plt.plot(col_values, rel_col_values, *fig_args, **fig_kwargs)
        else:
            plt.plot(col_values, rel_col_values, 'bo', **fig_kwargs)
        plt.xlabel(col_name)
        plt.ylabel(rel_col_name)
    plt.savefig(os.path.join(fig_folder, fig_name))
    dm.insert_image(fig_name, table_name, col_name, fig_title)
    return redirect(
        url_for('view_column', table_name=table_name, col_name=col_name))


@app.route('/del_figure/<name>')
def del_figure(name):
    dm.delete_image(name)
    os.remove(os.path.join('static/images', name + '.png'))
    return redirect(
        url_for('view_column', table_name=request.args.get('tn'), col_name=request.args.get('cn')))


@app.route('/add_target_column/<table_name>/<col_name>', methods=['POST'])
def add_target_column(table_name, col_name):
    df = dm.add_target_column(table_name, col_name, request.form['fill_na_method'])
    app.logger.debug(df)
    return redirect(url_for('view_column', table_name=table_name, col_name=col_name))


@app.route('/del_target_column/<table_name>/<col_name>')
def del_target_column(table_name, col_name):
    dm.del_target_column(table_name, col_name)
    return redirect(url_for('view_data_set', name=table_name))


if __name__ == '__main__':
    app.secret_key = 'super secret key'
    app.config['SESSION_TYPE'] = 'filesystem'
    # sess.init_app(app)
    app.run(debug=True, host="0.0.0.0")
