using System.Data;

namespace AptivDashboard.Gui;

internal sealed class EditTableForm : Form
{
    private readonly DataGridView _grid = new();
    private readonly Button _addButton = new();
    private readonly Button _deleteButton = new();
    private readonly Button _saveButton = new();
    private readonly Button _cancelButton = new();
    private readonly IReadOnlyDictionary<string, object?> _defaultValues;
    private readonly IReadOnlyList<string>? _timeOptions;

    public DataTable EditedTable { get; private set; }

    [System.ComponentModel.Browsable(false)]
    [System.ComponentModel.DesignerSerializationVisibility(System.ComponentModel.DesignerSerializationVisibility.Hidden)]
    public Func<DataTable, Task<bool>>? SaveAsync { get; set; }

    public EditTableForm(
        string title,
        DataTable source,
        string? note = null,
        IReadOnlyDictionary<string, object?>? defaultValues = null,
        IReadOnlyList<string>? timeOptions = null)
    {
        Text = title;
        Width = 860;
        Height = 560;
        StartPosition = FormStartPosition.CenterParent;
        _defaultValues = defaultValues ?? new Dictionary<string, object?>();
        _timeOptions = timeOptions;

        EditedTable = MakeEditableCopy(source);

        var root = new TableLayoutPanel
        {
            Dock = DockStyle.Fill,
            ColumnCount = 1,
            RowCount = note is null ? 2 : 3,
            Padding = new Padding(10),
        };
        if (note is not null)
        {
            root.RowStyles.Add(new RowStyle(SizeType.Absolute, 32));
        }
        root.RowStyles.Add(new RowStyle(SizeType.Percent, 100));
        root.RowStyles.Add(new RowStyle(SizeType.Absolute, 42));

        if (note is not null)
        {
            root.Controls.Add(new Label { Text = note, Dock = DockStyle.Fill, TextAlign = ContentAlignment.MiddleLeft }, 0, 0);
        }

        _grid.Dock = DockStyle.Fill;
        _grid.DataSource = EditedTable;
        _grid.ReadOnly = false;
        _grid.AllowUserToAddRows = true;
        _grid.AllowUserToDeleteRows = true;
        _grid.EditMode = DataGridViewEditMode.EditOnKeystrokeOrF2;
        _grid.AutoSizeColumnsMode = DataGridViewAutoSizeColumnsMode.Fill;
        _grid.DataBindingComplete += (_, _) => ConfigureColumns();
        _grid.DefaultValuesNeeded += (_, e) => ApplyDefaultValues(e.Row);

        var buttons = new FlowLayoutPanel
        {
            Dock = DockStyle.Fill,
            FlowDirection = FlowDirection.RightToLeft,
            WrapContents = false,
        };

        _addButton.Text = "+";
        _addButton.Width = 54;
        _addButton.Click += (_, _) =>
        {
            var row = EditedTable.NewRow();
            ApplyDefaultValues(row);
            EditedTable.Rows.Add(row);
        };

        _deleteButton.Text = "삭제";
        _deleteButton.Width = 72;
        _deleteButton.Click += (_, _) =>
        {
            foreach (DataGridViewRow row in _grid.SelectedRows)
            {
                if (!row.IsNewRow)
                {
                    _grid.Rows.Remove(row);
                }
            }
        };

        _saveButton.Text = "저장";
        _saveButton.Width = 84;
        _saveButton.Click += async (_, _) =>
        {
            EndGridEdit();
            if (SaveAsync is null)
            {
                DialogResult = DialogResult.OK;
                Close();
                return;
            }

            _saveButton.Enabled = false;
            try
            {
                await SaveAsync(EditedTable);
            }
            finally
            {
                _saveButton.Enabled = true;
            }
        };

        _cancelButton.Text = "닫기";
        _cancelButton.Width = 84;
        _cancelButton.DialogResult = DialogResult.Cancel;

        buttons.Controls.Add(_cancelButton);
        buttons.Controls.Add(_saveButton);
        buttons.Controls.Add(_deleteButton);
        buttons.Controls.Add(_addButton);

        var gridRow = note is null ? 0 : 1;
        var buttonRow = note is null ? 1 : 2;
        root.Controls.Add(_grid, 0, gridRow);
        root.Controls.Add(buttons, 0, buttonRow);

        AcceptButton = _saveButton;
        CancelButton = _cancelButton;
        Controls.Add(root);
    }

    public void SetSource(DataTable source)
    {
        EditedTable = MakeEditableCopy(source);
        _grid.DataSource = EditedTable;
    }

    private void EndGridEdit()
    {
        _grid.EndEdit();
        if (BindingContext is not null && BindingContext[EditedTable] is CurrencyManager manager)
        {
            manager.EndCurrentEdit();
        }
    }

    private static DataTable MakeEditableCopy(DataTable source)
    {
        var table = source.Copy();
        foreach (DataColumn column in table.Columns)
        {
            column.ReadOnly = false;
        }
        return table;
    }

    private void ConfigureColumns()
    {
        foreach (DataGridViewColumn column in _grid.Columns.Cast<DataGridViewColumn>().ToList())
        {
            if (column.Name is "from_time" or "to_time")
            {
                ReplaceWithCombo(column, _timeOptions ?? BuildTimeOptions());
            }
            else if (column.Name == "end_day")
            {
                ReplaceWithCalendar(column);
            }
            else if (column.Name == "shift_type")
            {
                ReplaceWithCombo(column, ["day", "night", "주간", "야간"]);
            }
        }
    }

    private void ApplyDefaultValues(DataGridViewRow row)
    {
        foreach (var (column, value) in _defaultValues)
        {
            if (_grid.Columns.Contains(column))
            {
                row.Cells[column].Value = value ?? "";
            }
        }
    }

    private void ApplyDefaultValues(DataRow row)
    {
        foreach (var (column, value) in _defaultValues)
        {
            if (EditedTable.Columns.Contains(column))
            {
                row[column] = value ?? "";
            }
        }
    }

    private void ReplaceWithCombo(DataGridViewColumn oldColumn, IEnumerable<string> values)
    {
        if (oldColumn is DataGridViewComboBoxColumn)
        {
            return;
        }

        var index = oldColumn.Index;
        var combo = new DataGridViewComboBoxColumn
        {
            Name = oldColumn.Name,
            HeaderText = oldColumn.HeaderText,
            DataPropertyName = oldColumn.DataPropertyName,
            FlatStyle = FlatStyle.Flat,
        };
        combo.Items.AddRange(values.Cast<object>().ToArray());
        _grid.Columns.RemoveAt(index);
        _grid.Columns.Insert(index, combo);
    }

    private void ReplaceWithCalendar(DataGridViewColumn oldColumn)
    {
        if (oldColumn is CalendarColumn)
        {
            return;
        }

        var index = oldColumn.Index;
        var calendar = new CalendarColumn
        {
            Name = oldColumn.Name,
            HeaderText = oldColumn.HeaderText,
            DataPropertyName = oldColumn.DataPropertyName,
        };
        _grid.Columns.RemoveAt(index);
        _grid.Columns.Insert(index, calendar);
    }

    private static string[] BuildTimeOptions()
    {
        var values = new List<string> { "" };
        for (var h = 0; h < 24; h++)
        {
            for (var m = 0; m < 60; m += 5)
            {
                values.Add($"{h:00}:{m:00}:00");
            }
        }
        return values.ToArray();
    }

    private sealed class CalendarColumn : DataGridViewColumn
    {
        public CalendarColumn() : base(new CalendarCell())
        {
        }
    }

    private sealed class CalendarCell : DataGridViewTextBoxCell
    {
        public override Type EditType => typeof(CalendarEditingControl);

        public override Type ValueType => typeof(string);

        public override object DefaultNewRowValue => DateTime.Today.ToString("yyyy-MM-dd");
    }

    private sealed class CalendarEditingControl : DateTimePicker, IDataGridViewEditingControl
    {
        private bool _valueChanged;

        public CalendarEditingControl()
        {
            Format = DateTimePickerFormat.Custom;
            CustomFormat = "yyyy-MM-dd";
        }

        [System.ComponentModel.Browsable(false)]
        [System.ComponentModel.DesignerSerializationVisibility(System.ComponentModel.DesignerSerializationVisibility.Hidden)]
        public DataGridView? EditingControlDataGridView { get; set; }

        [System.ComponentModel.Browsable(false)]
        [System.ComponentModel.DesignerSerializationVisibility(System.ComponentModel.DesignerSerializationVisibility.Hidden)]
#pragma warning disable CS8767
        public object EditingControlFormattedValue
        {
            get => Value.ToString("yyyy-MM-dd");
            set
            {
                var text = Convert.ToString(value) ?? "";
                var digits = new string(text.Where(char.IsDigit).ToArray());
                if (digits.Length >= 8 && DateTime.TryParseExact(digits[..8], "yyyyMMdd", null, System.Globalization.DateTimeStyles.None, out var exactDate))
                {
                    Value = exactDate;
                }
                else if (DateTime.TryParse(text, out var date))
                {
                    Value = date;
                }
            }
        }
#pragma warning restore CS8767

        [System.ComponentModel.Browsable(false)]
        [System.ComponentModel.DesignerSerializationVisibility(System.ComponentModel.DesignerSerializationVisibility.Hidden)]
        public int EditingControlRowIndex { get; set; }

        [System.ComponentModel.Browsable(false)]
        [System.ComponentModel.DesignerSerializationVisibility(System.ComponentModel.DesignerSerializationVisibility.Hidden)]
        public bool EditingControlValueChanged
        {
            get => _valueChanged;
            set => _valueChanged = value;
        }

        public Cursor EditingPanelCursor => base.Cursor;

        public bool RepositionEditingControlOnValueChange => false;

        public void ApplyCellStyleToEditingControl(DataGridViewCellStyle dataGridViewCellStyle)
        {
            Font = dataGridViewCellStyle.Font;
            CalendarForeColor = dataGridViewCellStyle.ForeColor;
            CalendarMonthBackground = dataGridViewCellStyle.BackColor;
        }

        public bool EditingControlWantsInputKey(Keys keyData, bool dataGridViewWantsInputKey)
        {
            return (keyData & Keys.KeyCode) is Keys.Left or Keys.Up or Keys.Down or Keys.Right or Keys.Home or Keys.End or Keys.PageDown or Keys.PageUp
                || !dataGridViewWantsInputKey;
        }

        public object GetEditingControlFormattedValue(DataGridViewDataErrorContexts context) => EditingControlFormattedValue;

        public void PrepareEditingControlForEdit(bool selectAll)
        {
        }

        protected override void OnValueChanged(EventArgs eventargs)
        {
            _valueChanged = true;
            EditingControlDataGridView?.NotifyCurrentCellDirty(true);
            base.OnValueChanged(eventargs);
        }
    }
}
