using System.Data;

namespace AptivDashboard.Gui;

public partial class Form1 : Form
{
    private readonly DateTimePicker _productionDayPicker = new();
    private readonly ComboBox _shiftBox = new();
    private readonly Button _currentButton = new();
    private readonly Button _refreshButton = new();
    private readonly Button _reconnectButton = new();
    private readonly Button _emailButton = new();
    private readonly Button _barcodeButton = new();
    private readonly Button _plannedButton = new();
    private readonly Button _stopButton = new();
    private readonly Label _statusLabel = new();
    private readonly TabControl _tabs = new();
    private readonly System.Windows.Forms.Timer _refreshTimer = new();
    private readonly string _databaseUrl = EnvFile.Get("DATABASE_URL");

    private readonly Dictionary<string, DataGridView> _grids = new();
    private NonOperationChart _nonopChart = null!;
    private PdBoardChart _pdBoardChart = null!;
    private DashboardReportSender _reportSender = null!;
    private DashboardScope? _manualScope;
    private bool _syncingScopeControls;
    private bool _refreshing;
    private bool _connected;
    private bool _configuringNonopGrid;
    private bool _savingNonopEdit;
    private bool _sparepartAlarmBaselineLoaded;
    private bool _sendingReport;
    private readonly HashSet<string> _seenSparepartAlarmKeys = new();
    private readonly HashSet<string> _sentAutoReportKeys = new();

    public Form1()
    {
        InitializeComponent();
        BuildUi();
        _reportSender = new DashboardReportSender(NewDatabaseClient);
        ConfigureTimer();
    }

    protected override async void OnShown(EventArgs e)
    {
        base.OnShown(e);
        await ReconnectAsync(showSuccess: false);
        await RefreshSelectedTabAsync();
    }

    private void BuildUi()
    {
        Text = "APTIV Dashboard";
        Width = 1500;
        Height = 920;
        MinimumSize = new Size(1180, 740);
        StartPosition = FormStartPosition.CenterScreen;

        var root = new TableLayoutPanel
        {
            Dock = DockStyle.Fill,
            ColumnCount = 1,
            RowCount = 3,
            Padding = new Padding(10),
        };
        root.RowStyles.Add(new RowStyle(SizeType.Absolute, 62));
        root.RowStyles.Add(new RowStyle(SizeType.Percent, 100));
        root.RowStyles.Add(new RowStyle(SizeType.Absolute, 24));

        var toolbar = new FlowLayoutPanel
        {
            Dock = DockStyle.Fill,
            FlowDirection = FlowDirection.LeftToRight,
            WrapContents = false,
            AutoScroll = true,
        };

        _productionDayPicker.Width = 130;
        _productionDayPicker.Format = DateTimePickerFormat.Custom;
        _productionDayPicker.CustomFormat = "yyyy-MM-dd";
        _productionDayPicker.Value = GetCurrentScopeParts(DateTime.Now).Date;
        _productionDayPicker.ValueChanged += (_, _) =>
        {
            if (!_syncingScopeControls)
            {
                _manualScope = SelectedScope();
            }
        };

        _shiftBox.Width = 90;
        _shiftBox.DropDownStyle = ComboBoxStyle.DropDownList;
        _shiftBox.Items.AddRange(new object[] { "주간", "야간" });
        _shiftBox.SelectedIndex = GetCurrentScopeParts(DateTime.Now).ShiftType == "day" ? 0 : 1;
        _shiftBox.SelectedIndexChanged += (_, _) =>
        {
            if (!_syncingScopeControls)
            {
                _manualScope = SelectedScope();
            }
        };

        _currentButton.Text = "현재 기준";
        _currentButton.Width = 90;
        _currentButton.Click += async (_, _) =>
        {
            _manualScope = null;
            ApplyScopeToControls(GetActiveScope());
            await RefreshSelectedTabAsync();
        };

        _refreshButton.Text = "조회";
        _refreshButton.Width = 72;
        _refreshButton.Click += async (_, _) =>
        {
            _manualScope = SelectedScope();
            await RefreshSelectedTabAsync();
        };

        _reconnectButton.Text = "DB 재연결";
        _reconnectButton.Width = 94;
        _reconnectButton.Click += async (_, _) => await ReconnectAsync(showSuccess: true);

        _emailButton.Text = "Email list";
        _emailButton.Width = 92;
        _emailButton.Click += async (_, _) => await EditEmailListAsync();

        _barcodeButton.Text = "Barcode";
        _barcodeButton.Width = 82;
        _barcodeButton.Click += async (_, _) => await EditBarcodeAsync();

        _plannedButton.Text = "계획정지시간";
        _plannedButton.Width = 112;
        _plannedButton.Click += async (_, _) => await EditPlannedAsync();

        _stopButton.Text = "생산 STOP";
        _stopButton.Width = 92;
        _stopButton.Click += async (_, _) => await SendManualReportAsync();

        toolbar.Controls.Add(Label("생산 일자"));
        toolbar.Controls.Add(_productionDayPicker);
        toolbar.Controls.Add(Label("Shift"));
        toolbar.Controls.Add(_shiftBox);
        toolbar.Controls.Add(_currentButton);
        toolbar.Controls.Add(_refreshButton);
        toolbar.Controls.Add(_reconnectButton);
        toolbar.Controls.Add(_emailButton);
        toolbar.Controls.Add(_barcodeButton);
        toolbar.Controls.Add(_plannedButton);
        toolbar.Controls.Add(_stopButton);

        foreach (var button in new[] { _currentButton, _refreshButton, _reconnectButton, _emailButton, _barcodeButton, _plannedButton, _stopButton })
        {
            StyleToolbarButton(button);
        }

        _tabs.Dock = DockStyle.Fill;
        _tabs.SelectedIndexChanged += async (_, _) => await RefreshSelectedTabAsync();
        BuildProductionStatusTab();
        BuildProductionInfoTab();
        BuildProductionAnalysisTab();
        BuildProgramStatusTab();

        _statusLabel.Dock = DockStyle.Fill;
        _statusLabel.TextAlign = ContentAlignment.MiddleLeft;

        root.Controls.Add(toolbar, 0, 0);
        root.Controls.Add(_tabs, 0, 1);
        root.Controls.Add(_statusLabel, 0, 2);
        Controls.Clear();
        Controls.Add(root);
    }

    private void ConfigureTimer()
    {
        _refreshTimer.Interval = 5000;
        _refreshTimer.Tick += async (_, _) =>
        {
            if (_manualScope is null)
            {
                ApplyScopeToControls(GetActiveScope());
            }
            await RefreshSelectedTabAsync();
            _ = CheckAutoReportScheduleAsync();
        };
        _refreshTimer.Start();
    }

    private void BuildProductionStatusTab()
    {
        var page = new TabPage("생산 현황") { Name = "status" };
        var split = new SplitContainer
        {
            Dock = DockStyle.Fill,
            Orientation = Orientation.Vertical,
            SplitterWidth = 4,
        };
        split.SizeChanged += (_, _) => ApplyStatusSplitRatio(split);
        _nonopChart = new NonOperationChart { Dock = DockStyle.Fill };
        split.Panel1.Controls.Add(_nonopChart);

        var right = new TableLayoutPanel
        {
            Dock = DockStyle.Fill,
            ColumnCount = 1,
            RowCount = 8,
            Padding = new Padding(6, 0, 0, 0),
        };
        right.RowStyles.Add(new RowStyle(SizeType.Absolute, 24));
        right.RowStyles.Add(new RowStyle(SizeType.Absolute, 340));
        right.RowStyles.Add(new RowStyle(SizeType.Absolute, 138));
        right.RowStyles.Add(new RowStyle(SizeType.Absolute, 0));
        right.RowStyles.Add(new RowStyle(SizeType.Absolute, 24));
        right.RowStyles.Add(new RowStyle(SizeType.Absolute, 72));
        right.RowStyles.Add(new RowStyle(SizeType.Absolute, 24));
        right.RowStyles.Add(new RowStyle(SizeType.Percent, 100));

        right.Controls.Add(SectionLabel("비가동 시간 상세"), 0, 0);
        right.Controls.Add(CreateGrid("nonop_detail"), 0, 1);
        var workerPanel = WorkerPanel();
        right.Controls.Add(workerPanel, 0, 2);
        right.SetRowSpan(workerPanel, 2);
        right.Controls.Add(SectionLabel("Master Sample"), 0, 4);
        right.Controls.Add(CreateGrid("mastersample"), 0, 5);
        right.Controls.Add(SectionLabel("비가동 전체 (Minor제외)"), 0, 6);
        right.Controls.Add(CreateGrid("nonop_all"), 0, 7);

        split.Panel2.Controls.Add(right);
        page.Controls.Add(split);
        page.SizeChanged += (_, _) => ApplyStatusSplitRatio(split);
        _tabs.TabPages.Add(page);
    }

    private void BuildProductionInfoTab()
    {
        var page = new TabPage("생산 정보") { Name = "info" };
        var container = new Panel
        {
            Dock = DockStyle.Fill,
            BorderStyle = BorderStyle.FixedSingle,
            Padding = new Padding(4),
        };
        var flow = new FlowLayoutPanel
        {
            Dock = DockStyle.Fill,
            AutoScroll = true,
            FlowDirection = FlowDirection.TopDown,
            WrapContents = false,
            Padding = new Padding(0),
        };

        var firstRow = new FlowLayoutPanel
        {
            AutoSize = true,
            AutoSizeMode = AutoSizeMode.GrowAndShrink,
            FlowDirection = FlowDirection.LeftToRight,
            WrapContents = false,
            Margin = new Padding(0, 0, 0, 4),
        };
        firstRow.Controls.Add(ProductionInfoSection("OEE Total", "oee_total"));
        firstRow.Controls.Add(ProductionInfoSection("OEE Line", "oee_line"));
        firstRow.Controls.Add(ProductionInfoSection("OEE Station", "oee_station"));
        flow.Controls.Add(firstRow);

        var sections = new (string Title, string Key)[]
        {
            ("계획 정지 시간", "planned_stop"),
            ("비가동 시간", "non_time"),
            ("품번별 총 생산량", "final_amount"),
            ("TEST 합격률", "pass_percent"),
            ("FAIL LIST 1회", "fct_fail_1"),
            ("FAIL LIST 2회", "fct_fail_2"),
            ("FAIL LIST 3회", "fct_fail_3"),
        };

        for (var i = 0; i < sections.Length; i++)
        {
            flow.Controls.Add(ProductionInfoSection(sections[i].Title, sections[i].Key));
        }

        flow.SizeChanged += (_, _) => ResizeProductionInfoSections(flow, firstRow);
        container.Controls.Add(flow);
        page.Controls.Add(container);
        _tabs.TabPages.Add(page);
    }

    private void BuildProductionAnalysisTab()
    {
        var page = new TabPage("생산 분석") { Name = "analysis" };
        var container = new Panel
        {
            Dock = DockStyle.Fill,
            BorderStyle = BorderStyle.FixedSingle,
            Padding = new Padding(4),
        };
        var flow = new FlowLayoutPanel
        {
            Dock = DockStyle.Fill,
            AutoScroll = true,
            FlowDirection = FlowDirection.TopDown,
            WrapContents = false,
            Padding = new Padding(0),
        };

        flow.Controls.Add(ProductionInfoSection("Sparepart 알람", "alarm_record"));
        flow.Controls.Add(PdBoardSection());
        flow.Controls.Add(ProductionInfoSection("FCT Worst Case", "worst_case"));
        flow.Controls.Add(ProductionInfoSection("조립 불량 손실", "afa_wasted"));
        flow.Controls.Add(ProductionInfoSection("MES 불량 손실", "mes_wasted"));

        flow.SizeChanged += (_, _) => ResizeProductionInfoSections(flow, null);
        container.Controls.Add(flow);
        page.Controls.Add(container);
        _tabs.TabPages.Add(page);
    }

    private void BuildProgramStatusTab()
    {
        var page = new TabPage("프로그램 상태") { Name = "program" };
        page.Controls.Add(CreateGrid("demon_health"));
        _tabs.TabPages.Add(page);
    }

    private TabPage GridPage(string title, string key, bool saveButton = false)
    {
        var page = new TabPage(title);
        if (!saveButton)
        {
            page.Controls.Add(CreateGrid(key));
            return page;
        }

        var root = new TableLayoutPanel { Dock = DockStyle.Fill, ColumnCount = 1, RowCount = 2 };
        root.RowStyles.Add(new RowStyle(SizeType.Absolute, 38));
        root.RowStyles.Add(new RowStyle(SizeType.Percent, 100));
        var buttons = new FlowLayoutPanel
        {
            Dock = DockStyle.Fill,
            FlowDirection = FlowDirection.RightToLeft,
            WrapContents = false,
        };
        var saveButton2 = new Button { Text = "저장", Width = 84, Height = 30, Margin = new Padding(4) };
        saveButton2.Click += async (_, _) => await SaveWorkerInfoAsync();
        buttons.Controls.Add(saveButton2);
        if (key == "worker_info")
        {
            var addButton = new Button { Text = "+", Width = 54, Height = 30, Margin = new Padding(4) };
            addButton.Click += (_, _) => AddWorkerInfoRow();
            buttons.Controls.Add(addButton);
        }
        root.Controls.Add(buttons, 0, 0);
        root.Controls.Add(CreateGrid(key, readOnly: false), 0, 1);
        page.Controls.Add(root);
        return page;
    }

    private Control ProductionInfoSection(string title, string key)
    {
        var panel = new TableLayoutPanel
        {
            AutoSize = true,
            AutoSizeMode = AutoSizeMode.GrowAndShrink,
            ColumnCount = 1,
            RowCount = 2,
            Margin = new Padding(3),
        };
        panel.RowStyles.Add(new RowStyle(SizeType.Absolute, 24));
        panel.RowStyles.Add(new RowStyle(SizeType.AutoSize));

        panel.Controls.Add(SectionLabel(title), 0, 0);
        var grid = CreateGrid(key);
        grid.Dock = DockStyle.None;
        panel.Controls.Add(grid, 0, 1);
        return panel;
    }

    private Control PdBoardSection()
    {
        var panel = new TableLayoutPanel
        {
            AutoSize = true,
            AutoSizeMode = AutoSizeMode.GrowAndShrink,
            ColumnCount = 1,
            RowCount = 3,
            Margin = new Padding(3),
        };
        panel.RowStyles.Add(new RowStyle(SizeType.Absolute, 24));
        panel.RowStyles.Add(new RowStyle(SizeType.AutoSize));
        panel.RowStyles.Add(new RowStyle(SizeType.Absolute, 450));

        panel.Controls.Add(SectionLabel("PD-Board 열화 모니터링(WARING은 교체 검토 필요 및 모니터링 필요/ CRITICAL은 교체 필요)"), 0, 0);

        var grid = CreateGrid("pd_board");
        grid.Dock = DockStyle.None;
        panel.Controls.Add(grid, 0, 1);

        _pdBoardChart = new PdBoardChart
        {
            Dock = DockStyle.None,
            Height = 440,
            Margin = new Padding(0, 6, 0, 0),
        };
        panel.Controls.Add(_pdBoardChart, 0, 2);
        return panel;
    }

    private static void ResizeProductionInfoSections(FlowLayoutPanel flow, Control? firstRow)
    {
        var width = Math.Max(300, flow.ClientSize.Width - SystemInformation.VerticalScrollBarWidth - flow.Padding.Horizontal - 4);
        if (firstRow is not null)
        {
            firstRow.Width = width;
        }
        var firstRowSectionWidth = Math.Max(220, (width - 18) / 3);

        foreach (Control control in flow.Controls)
        {
            if (firstRow is not null && control == firstRow)
            {
                foreach (Control section in firstRow.Controls)
                {
                    section.Width = firstRowSectionWidth;
                    ResizeProductionInfoGrid(section);
                }
            }
            else
            {
                control.Width = width;
                ResizeProductionInfoGrid(control);
            }
        }
    }

    private static void ResizeProductionInfoGrid(Control? section)
    {
        if (section is null)
        {
            return;
        }

        var grid = section.Controls.OfType<DataGridView>().FirstOrDefault();
        if (grid is null)
        {
            return;
        }

        grid.AutoResizeColumns(DataGridViewAutoSizeColumnsMode.DisplayedCells);
        grid.AutoResizeRows(DataGridViewAutoSizeRowsMode.DisplayedCells);

        var columnWidth = grid.RowHeadersVisible ? grid.RowHeadersWidth : 0;
        foreach (DataGridViewColumn column in grid.Columns)
        {
            if (column.Visible)
            {
                columnWidth += column.Width;
            }
        }

        var rowHeight = grid.ColumnHeadersVisible ? grid.ColumnHeadersHeight : 0;
        foreach (DataGridViewRow row in grid.Rows)
        {
            if (row.Visible)
            {
                rowHeight += row.Height;
            }
        }

        grid.Width = Math.Max(section.Width - 6, columnWidth + 4);
        grid.Height = Math.Max(56, rowHeight + 4);

        var chart = section.Controls.OfType<PdBoardChart>().FirstOrDefault();
        if (chart is not null)
        {
            chart.Width = Math.Max(section.Width - 6, 640);
            chart.Height = 440;
            section.Height = chart.Top + chart.Height + 8;
            chart.Invalidate();
            return;
        }

        section.Height = grid.Top + grid.Height + 4;
    }

    private DataGridView CreateGrid(string key, bool readOnly = true)
    {
        var isNonopDetail = key == "nonop_detail";
        var compactStatusGrid = key is "nonop_detail" or "worker_info" or "mastersample" or "nonop_all";
        var isFlowGrid = IsFlowGrid(key);
        var grid = new DataGridView
        {
            Dock = DockStyle.Fill,
            ReadOnly = isNonopDetail ? false : readOnly,
            AllowUserToAddRows = !readOnly && !isNonopDetail,
            AllowUserToDeleteRows = !readOnly && !isNonopDetail,
            AutoSizeColumnsMode = compactStatusGrid ? DataGridViewAutoSizeColumnsMode.Fill : DataGridViewAutoSizeColumnsMode.DisplayedCells,
            SelectionMode = DataGridViewSelectionMode.FullRowSelect,
            MultiSelect = false,
            ScrollBars = isFlowGrid ? ScrollBars.None : ScrollBars.Both,
            BorderStyle = isFlowGrid ? BorderStyle.None : BorderStyle.Fixed3D,
            BackgroundColor = SystemColors.Control,
        };
        if (isNonopDetail)
        {
            grid.DataBindingComplete += (_, _) => ConfigureNonopDetailGrid(grid);
            grid.EditingControlShowing += NonopDetailEditingControlShowing;
            grid.CellValueChanged += async (_, e) => await NonopDetailCellValueChangedAsync(grid, e);
            grid.CurrentCellDirtyStateChanged += (_, _) =>
            {
                if (grid.IsCurrentCellDirty)
                {
                    grid.CommitEdit(DataGridViewDataErrorContexts.Commit);
                }
            };
            grid.DataError += (_, e) => e.ThrowException = false;
        }
        else if (key == "worker_info")
        {
            grid.DataBindingComplete += (_, _) =>
            {
                ConfigureWorkerInfoGrid(grid);
                ApplyCompactColumnWeights(grid);
            };
            grid.DefaultValuesNeeded += (_, e) => FillWorkerScopeDefaults(e.Row);
            grid.UserAddedRow += (_, e) => FillWorkerScopeDefaults(e.Row);
        }
        else if (compactStatusGrid)
        {
            grid.DataBindingComplete += (_, _) => ApplyCompactColumnWeights(grid);
        }
        else if (isFlowGrid)
        {
            grid.DataBindingComplete += (_, _) => ResizeProductionInfoGrid(grid.Parent);
        }
        _grids[key] = grid;
        return grid;
    }

    private async Task ReconnectAsync(bool showSuccess)
    {
        try
        {
            var db = NewDatabaseClient();
            var who = await db.PingAsync();
            _connected = true;
            _statusLabel.Text = $"DB 연결 OK: {who}";
            if (showSuccess)
            {
                MessageBox.Show(this, "DB에 다시 연결되었습니다.", "DB 연결", MessageBoxButtons.OK, MessageBoxIcon.Information);
            }
        }
        catch (Exception ex)
        {
            _connected = false;
            _statusLabel.Text = $"DB 연결 실패: {ex.Message}";
            MessageBox.Show(this, $"DB 연결이 안 됩니다.\n\n{ex.Message}", "DB 연결 실패", MessageBoxButtons.OK, MessageBoxIcon.Error);
        }
    }

    private async Task RefreshSelectedTabAsync()
    {
        if (_refreshing || !_connected)
        {
            return;
        }

        _refreshing = true;
        SetBusy(true);
        try
        {
            var scope = GetActiveScope();

            switch (_tabs.SelectedTab?.Name)
            {
                case "status":
                    await RefreshProductionStatusAsync(scope);
                    break;
                case "info":
                    await RefreshProductionInfoAsync(scope);
                    break;
                case "analysis":
                    await RefreshProductionAnalysisAsync(scope);
                    break;
                case "program":
                    await RefreshProgramAsync();
                    break;
            }
        }
        catch (Exception ex)
        {
            _connected = false;
            _statusLabel.Text = $"조회 실패: {ex.Message}";
            MessageBox.Show(this, $"DB 조회 중 오류가 발생했습니다.\n\n{ex.Message}", "조회 실패", MessageBoxButtons.OK, MessageBoxIcon.Error);
        }
        finally
        {
            SetBusy(false);
            _refreshing = false;
        }
    }

    private async Task RefreshProductionStatusAsync(DashboardScope scope)
    {
        var db = NewDatabaseClient();
        var nonop = await db.GetNonOperationAsync(scope, reasonOnly: false);
        var planned = await db.GetPlannedTimeAsync(scope.ProdDay);
        _nonopChart.SetData(nonop, planned, scope);
        if (!_grids["nonop_detail"].ContainsFocus)
        {
            _grids["nonop_detail"].DataSource = nonop;
        }
        _grids["nonop_all"].DataSource = await db.GetNonOperationAsync(scope, reasonOnly: true);
        if (!_grids["worker_info"].ContainsFocus)
        {
            var worker = await db.GetWorkerInfoAsync(scope);
            EnsureWorkerSeedRow(worker, scope);
            _grids["worker_info"].DataSource = worker;
        }
        _grids["mastersample"].DataSource = await db.GetReportAsync("mastersample", scope);
        _statusLabel.Text = $"생산 현황 갱신: {DateTime.Now:HH:mm:ss}";
    }

    private async Task RefreshProductionInfoAsync(DashboardScope scope)
    {
        var db = NewDatabaseClient();
        foreach (var key in new[] { "oee_total", "oee_line", "oee_station", "planned_stop", "non_time", "final_amount", "pass_percent", "fct_fail_1", "fct_fail_2", "fct_fail_3" })
        {
            var table = await db.GetReportAsync(key, scope);
            RemoveEmptyValueColumns(table);
            BindFlowGrid(key, table);
        }
        _statusLabel.Text = $"생산 정보 갱신: {DateTime.Now:HH:mm:ss}";
    }

    private async Task RefreshProductionAnalysisAsync(DashboardScope scope)
    {
        var db = NewDatabaseClient();
        var alarm = await db.GetRecentAlarmsAsync(scope.ProdDay);
        ShowNewSparepartAlarms(alarm);
        RemoveEmptyValueColumns(alarm);
        BindFlowGrid("alarm_record", alarm);

        var pdBoard = await db.GetPdBoardAsync(scope.ProdDay);
        _pdBoardChart.SetData(pdBoard);
        var pdBoardDisplay = SelectColumns(pdBoard, "end_day", "station", "last_status");
        RemoveEmptyValueColumns(pdBoardDisplay);
        BindFlowGrid("pd_board", pdBoardDisplay);

        foreach (var key in new[] { "worst_case", "afa_wasted", "mes_wasted" })
        {
            var table = await db.GetReportAsync(key, scope);
            RemoveEmptyValueColumns(table);
            BindFlowGrid(key, table);
        }
        _statusLabel.Text = $"생산 분석 갱신: {DateTime.Now:HH:mm:ss}";
    }

    private async Task RefreshProgramAsync()
    {
        _grids["demon_health"].DataSource = await NewDatabaseClient().GetDemonHealthAsync();
        _statusLabel.Text = $"프로그램 상태 갱신: {DateTime.Now:HH:mm:ss}";
    }

    private async Task SendManualReportAsync()
    {
        if (_sendingReport)
        {
            return;
        }

        _sendingReport = true;
        _stopButton.Enabled = false;
        try
        {
            var scope = GetActiveScope();
            await _reportSender.SendAsync(scope, "생산 STOP");
            _statusLabel.Text = $"보고서 메일 발송 완료: {DateTime.Now:HH:mm:ss}";
            MessageBox.Show(this, "PDF 보고서가 생성되어 Email list로 발송되었습니다.", "생산 STOP", MessageBoxButtons.OK, MessageBoxIcon.Information);
        }
        catch (Exception ex)
        {
            _statusLabel.Text = $"보고서 메일 발송 실패: {ex.Message}";
            MessageBox.Show(this, $"보고서 메일 발송 중 오류가 발생했습니다.\n\n{ex.Message}", "생산 STOP", MessageBoxButtons.OK, MessageBoxIcon.Error);
        }
        finally
        {
            _stopButton.Enabled = true;
            _sendingReport = false;
        }
    }

    private Task CheckAutoReportScheduleAsync()
    {
        var now = DateTime.Now;
        var dayBoundary = now.Date.AddHours(8).AddMinutes(30);
        var nightBoundary = now.Date.AddHours(20).AddMinutes(30);

        if (now >= nightBoundary && now < nightBoundary.AddMinutes(1))
        {
            var scope = new DashboardScope(now.Date.ToString("yyyyMMdd"), "day");
            return StartAutoReportAsync(scope, "주간 자동 발송", $"day|{scope.ProdDay}");
        }

        if (now >= dayBoundary && now < dayBoundary.AddMinutes(1))
        {
            var prodDay = now.Date.AddDays(-1).ToString("yyyyMMdd");
            var scope = new DashboardScope(prodDay, "night");
            return StartAutoReportAsync(scope, "야간 자동 발송", $"night|{scope.ProdDay}");
        }

        return Task.CompletedTask;
    }

    private Task StartAutoReportAsync(DashboardScope scope, string trigger, string key)
    {
        if (_sendingReport || !_sentAutoReportKeys.Add(key))
        {
            return Task.CompletedTask;
        }

        _sendingReport = true;
        _ = Task.Run(async () =>
        {
            try
            {
                await _reportSender.SendAsync(scope, trigger);
                BeginInvoke(() => _statusLabel.Text = $"{trigger} 완료: {DateTime.Now:HH:mm:ss}");
            }
            catch (Exception ex)
            {
                BeginInvoke(() => _statusLabel.Text = $"{trigger} 실패: {ex.Message}");
            }
            finally
            {
                _sendingReport = false;
            }
        });

        return Task.CompletedTask;
    }

    private async Task EditEmailListAsync()
    {
        var db = NewDatabaseClient();
        var original = await db.GetEmailListAsync();
        using var form = new EditTableForm("Email list", original);
        var keepEmailPopupOpen = true;
        if (keepEmailPopupOpen)
        {
            form.SaveAsync = async edited =>
            {
                if (!ConfirmEditPassword("Email list 저장"))
                {
                    return false;
                }
                await db.SaveEmailListAsync(original, edited);
                original = await db.GetEmailListAsync();
                form.SetSource(original);
                await RefreshSelectedTabAsync();
                return true;
            };
            form.ShowDialog(this);
            return;
        }
        if (form.ShowDialog(this) == DialogResult.OK)
        {
            if (!ConfirmEditPassword("Email list 저장"))
            {
                return;
            }
            await db.SaveEmailListAsync(original, form.EditedTable);
            await RefreshSelectedTabAsync();
        }
    }

    private async Task EditBarcodeAsync()
    {
        var db = NewDatabaseClient();
        var original = await db.GetRemarkInfoAsync();
        using var form = new EditTableForm("Barcode", original, "※ 전체 바코드에 18번째 문자를 넣을 것");
        var keepBarcodePopupOpen = true;
        if (keepBarcodePopupOpen)
        {
            form.SaveAsync = async edited =>
            {
                if (!ConfirmEditPassword("Barcode 저장"))
                {
                    return false;
                }
                await db.SaveRemarkInfoAsync(original, edited);
                original = await db.GetRemarkInfoAsync();
                form.SetSource(original);
                await RefreshSelectedTabAsync();
                return true;
            };
            form.ShowDialog(this);
            return;
        }
        if (form.ShowDialog(this) == DialogResult.OK)
        {
            if (!ConfirmEditPassword("Barcode 저장"))
            {
                return;
            }
            await db.SaveRemarkInfoAsync(original, form.EditedTable);
            await RefreshSelectedTabAsync();
        }
    }

    private async Task EditPlannedAsync()
    {
        try
        {
            var db = NewDatabaseClient();
            var current = GetCurrentScopeParts(DateTime.Now);
            var scope = new DashboardScope(current.Date.ToString("yyyyMMdd"), current.ShiftType);
            var original = await db.GetPlannedTimeAsync(scope.ProdDay, scope.ShiftType);
            var popupTable = original.Copy();
            MakeTableEditable(popupTable);
            EnsurePlannedEditRows(popupTable, scope.ProdDay, 4);
            using var form = new EditTableForm("계획정지시간", popupTable, defaultValues: new Dictionary<string, object?>
            {
                ["end_day"] = scope.ProdDay.Insert(6, "-").Insert(4, "-"),
            }, timeOptions: BuildPlannedTimeOptions(scope.ShiftType));
            var keepPlannedPopupOpen = true;
            if (keepPlannedPopupOpen)
            {
                form.SaveAsync = async edited =>
                {
                    await db.SavePlannedTimeAsync(original, edited);
                    original = await db.GetPlannedTimeAsync(scope.ProdDay, scope.ShiftType);
                    var refreshed = original.Copy();
                    MakeTableEditable(refreshed);
                    EnsurePlannedEditRows(refreshed, scope.ProdDay, 4);
                    form.SetSource(refreshed);
                    await RefreshSelectedTabAsync();
                    return true;
                };
                form.ShowDialog(this);
                return;
            }
            if (form.ShowDialog(this) == DialogResult.OK)
            {
                await db.SavePlannedTimeAsync(original, form.EditedTable);
                await RefreshSelectedTabAsync();
            }
        }
        catch (Exception ex)
        {
            MessageBox.Show(this, $"계획정지시간 팝업을 여는 중 오류가 발생했습니다.\n\n{ex.Message}", "계획정지시간", MessageBoxButtons.OK, MessageBoxIcon.Error);
        }
    }

    private async Task SaveWorkerInfoAsync()
    {
        if (_grids["worker_info"].DataSource is not DataTable edited)
        {
            return;
        }

        ApplyWorkerScopeDefaults(edited);

        var db = NewDatabaseClient();
        var original = await db.GetWorkerInfoAsync(GetActiveScope());
        await db.SaveWorkerInfoAsync(original, edited);
        await RefreshSelectedTabAsync();
    }

    private void AddWorkerInfoRow()
    {
        if (_grids["worker_info"].DataSource is not DataTable table)
        {
            table = CreateWorkerInfoTable();
            _grids["worker_info"].DataSource = table;
        }

        var scope = GetActiveScope();
        var row = table.NewRow();
        row["end_day"] = scope.ProdDay;
        row["shift_type"] = scope.ShiftType;
        row["worker_name"] = "";
        row["order_number"] = "";
        table.Rows.Add(row);

        var grid = _grids["worker_info"];
        var rowIndex = grid.Rows.Count - 1;
        if (grid.AllowUserToAddRows && rowIndex > 0)
        {
            rowIndex -= 1;
        }
        if (rowIndex >= 0)
        {
            grid.CurrentCell = grid.Rows[rowIndex].Cells["worker_name"];
            grid.BeginEdit(true);
        }
    }

    private static void EnsureWorkerSeedRow(DataTable table, DashboardScope scope)
    {
        if (table.Rows.Count > 0)
        {
            return;
        }

        var row = table.NewRow();
        row["end_day"] = scope.ProdDay;
        row["shift_type"] = scope.ShiftType;
        row["worker_name"] = "";
        row["order_number"] = "";
        table.Rows.Add(row);
    }

    private DatabaseClient NewDatabaseClient() => new(_databaseUrl);

    private DashboardScope GetActiveScope() => _manualScope ?? CurrentScope(DateTime.Now);

    private DashboardScope SelectedScope()
    {
        var shift = _shiftBox.SelectedIndex == 1 ? "night" : "day";
        return new DashboardScope(_productionDayPicker.Value.ToString("yyyyMMdd"), shift);
    }

    private void ApplyScopeToControls(DashboardScope scope)
    {
        _syncingScopeControls = true;
        try
        {
            _productionDayPicker.Value = DateTime.ParseExact(scope.ProdDay, "yyyyMMdd", null);
            _shiftBox.SelectedIndex = scope.ShiftType == "night" ? 1 : 0;
        }
        finally
        {
            _syncingScopeControls = false;
        }
    }

    private static (DateTime Date, string ShiftType) GetCurrentScopeParts(DateTime now)
    {
        var dayStart = now.Date.AddHours(8).AddMinutes(30);
        var nightStart = now.Date.AddHours(20).AddMinutes(30);
        if (now >= dayStart && now < nightStart)
        {
            return (now.Date, "day");
        }
        if (now >= nightStart)
        {
            return (now.Date, "night");
        }
        return (now.Date.AddDays(-1), "night");
    }

    private static DashboardScope CurrentScope(DateTime now)
    {
        var current = GetCurrentScopeParts(now);
        return new DashboardScope(current.Date.ToString("yyyyMMdd"), current.ShiftType);
    }

    private static DataTable LimitRows(DataTable source, int maxRows)
    {
        var copy = source.Clone();
        foreach (DataRow row in source.Rows.Cast<DataRow>().Take(maxRows))
        {
            copy.ImportRow(row);
        }
        return copy;
    }

    private void SetBusy(bool busy)
    {
        _refreshButton.Enabled = !busy;
        _reconnectButton.Enabled = !busy;
        Cursor = Cursors.Default;
    }

    private void ConfigureNonopDetailGrid(DataGridView grid)
    {
        if (_configuringNonopGrid)
        {
            return;
        }

        _configuringNonopGrid = true;
        try
        {
            foreach (DataGridViewColumn column in grid.Columns)
            {
                column.ReadOnly = column.Name is not ("reason" or "sparepart");
            }

            ReplaceColumnWithCombo(grid, "reason", ["", "sparepart 교체", "기타"], allowFreeText: true);
            ReplaceColumnWithCombo(grid, "sparepart", ["", "usb_c", "usb_a", "mini_b", "probe_pin"], allowFreeText: false);
            ApplyCompactColumnWeights(grid);
        }
        finally
        {
            _configuringNonopGrid = false;
        }
    }

    private static void ReplaceColumnWithCombo(DataGridView grid, string name, string[] values, bool allowFreeText)
    {
        if (!grid.Columns.Contains(name) || grid.Columns[name] is DataGridViewComboBoxColumn)
        {
            return;
        }

        var old = grid.Columns[name]!;
        var index = old.Index;
        var combo = new DataGridViewComboBoxColumn
        {
            Name = old.Name,
            HeaderText = old.HeaderText,
            DataPropertyName = old.DataPropertyName,
            ReadOnly = false,
            FlatStyle = FlatStyle.Flat,
            DisplayStyle = DataGridViewComboBoxDisplayStyle.DropDownButton,
            Tag = allowFreeText ? "free" : "list",
        };
        combo.Items.AddRange(values.Cast<object>().ToArray());
        grid.Columns.RemoveAt(index);
        grid.Columns.Insert(index, combo);
    }

    private static void NonopDetailEditingControlShowing(object? sender, DataGridViewEditingControlShowingEventArgs e)
    {
        if (sender is not DataGridView grid || e.Control is not ComboBox combo || grid.CurrentCell is null)
        {
            return;
        }

        var column = grid.Columns[grid.CurrentCell.ColumnIndex];
        combo.DropDownStyle = column.Name == "reason" ? ComboBoxStyle.DropDown : ComboBoxStyle.DropDownList;
    }

    private async Task NonopDetailCellValueChangedAsync(DataGridView grid, DataGridViewCellEventArgs e)
    {
        if (_configuringNonopGrid || _savingNonopEdit || e.RowIndex < 0 || e.ColumnIndex < 0)
        {
            return;
        }

        var columnName = grid.Columns[e.ColumnIndex].Name;
        if (columnName is not ("reason" or "sparepart"))
        {
            return;
        }

        var row = grid.Rows[e.RowIndex];
        if (row.IsNewRow)
        {
            return;
        }

        _savingNonopEdit = true;
        try
        {
            var reason = CellText(row, "reason");
            var sparepart = CellText(row, "sparepart");
            if (columnName == "reason" && reason != "sparepart 교체")
            {
                sparepart = "";
                row.Cells["sparepart"].Value = "";
            }

            await NewDatabaseClient().UpdateNonOperationReasonAsync(
                CellText(row, "prod_day"),
                CellText(row, "shift_type"),
                CellText(row, "station"),
                CellText(row, "from_ts"),
                CellText(row, "to_ts"),
                reason,
                sparepart);

            _statusLabel.Text = $"비가동 사유 저장: {DateTime.Now:HH:mm:ss}";
        }
        catch (Exception ex)
        {
            MessageBox.Show(this, $"비가동 사유 저장 실패\n\n{ex.Message}", "저장 실패", MessageBoxButtons.OK, MessageBoxIcon.Error);
        }
        finally
        {
            _savingNonopEdit = false;
        }
    }

    private static string CellText(DataGridViewRow row, string columnName)
    {
        return Convert.ToString(row.Cells[columnName].Value)?.Trim() ?? "";
    }

    private static void ApplyCompactColumnWeights(DataGridView grid)
    {
        foreach (DataGridViewColumn column in grid.Columns)
        {
            column.AutoSizeMode = DataGridViewAutoSizeColumnMode.Fill;
            column.FillWeight = column.Name switch
            {
                "prod_day" or "end_day" => 90,
                "shift_type" => 80,
                "station" => 75,
                "from_ts" or "to_ts" => 80,
                "reason" => 95,
                "sparepart" => 95,
                "worker_name" => 120,
                "order_number" => 120,
                "Mastersample" or "first_time" => 110,
                _ => 80,
            };
        }
    }

    private void ConfigureWorkerInfoGrid(DataGridView grid)
    {
        foreach (DataGridViewColumn column in grid.Columns)
        {
            column.ReadOnly = column.Name is "end_day" or "shift_type";
        }
    }

    private void FillWorkerScopeDefaults(DataGridViewRow row)
    {
        if (row.IsNewRow)
        {
            var scope = GetActiveScope();
            row.Cells["end_day"].Value = scope.ProdDay;
            row.Cells["shift_type"].Value = scope.ShiftType;
        }
    }

    private void ApplyWorkerScopeDefaults(DataTable table)
    {
        var scope = GetActiveScope();
        foreach (DataRow row in table.Rows)
        {
            if (row.RowState == DataRowState.Deleted)
            {
                continue;
            }

            row["end_day"] = scope.ProdDay;
            row["shift_type"] = scope.ShiftType;
        }
    }

    private static DataTable CreateWorkerInfoTable()
    {
        var table = new DataTable();
        table.Columns.Add("end_day", typeof(string));
        table.Columns.Add("shift_type", typeof(string));
        table.Columns.Add("worker_name", typeof(string));
        table.Columns.Add("order_number", typeof(string));
        return table;
    }

    private static void EnsurePlannedEditRows(DataTable table, string prodDay, int rows)
    {
        var day = prodDay.Insert(6, "-").Insert(4, "-");
        while (table.Rows.Count < rows)
        {
            var row = table.NewRow();
            if (table.Columns.Contains("end_day"))
            {
                row["end_day"] = day;
            }
            table.Rows.Add(row);
        }
    }

    private static string[] BuildPlannedTimeOptions(string shiftType)
    {
        var options = new List<string> { "" };
        if (shiftType == "night")
        {
            AddTimeOptions(options, new TimeSpan(20, 30, 0), new TimeSpan(23, 55, 0));
            AddTimeOptions(options, TimeSpan.Zero, new TimeSpan(8, 25, 0));
            options.Add("08:29:59");
        }
        else
        {
            AddTimeOptions(options, new TimeSpan(8, 30, 0), new TimeSpan(20, 25, 0));
            options.Add("20:29:59");
        }

        return options.ToArray();
    }

    private static void AddTimeOptions(List<string> options, TimeSpan start, TimeSpan last)
    {
        for (var time = start; time <= last; time = time.Add(TimeSpan.FromMinutes(5)))
        {
            options.Add($"{(int)time.TotalHours % 24:00}:{time.Minutes:00}:00");
        }
    }

    private static void MakeTableEditable(DataTable table)
    {
        foreach (DataColumn column in table.Columns)
        {
            column.ReadOnly = false;
        }
    }

    private bool ConfirmEditPassword(string title)
    {
        using var form = new Form
        {
            Text = title,
            Width = 360,
            Height = 150,
            FormBorderStyle = FormBorderStyle.FixedDialog,
            StartPosition = FormStartPosition.CenterParent,
            MaximizeBox = false,
            MinimizeBox = false,
        };

        var root = new TableLayoutPanel
        {
            Dock = DockStyle.Fill,
            ColumnCount = 1,
            RowCount = 3,
            Padding = new Padding(12),
        };
        root.RowStyles.Add(new RowStyle(SizeType.Absolute, 26));
        root.RowStyles.Add(new RowStyle(SizeType.Absolute, 30));
        root.RowStyles.Add(new RowStyle(SizeType.Absolute, 40));

        var input = new TextBox
        {
            Dock = DockStyle.Fill,
            UseSystemPasswordChar = true,
        };
        var buttons = new FlowLayoutPanel
        {
            Dock = DockStyle.Fill,
            FlowDirection = FlowDirection.RightToLeft,
        };
        var ok = new Button { Text = "확인", DialogResult = DialogResult.OK, Width = 76 };
        var cancel = new Button { Text = "취소", DialogResult = DialogResult.Cancel, Width = 76 };
        buttons.Controls.Add(ok);
        buttons.Controls.Add(cancel);

        root.Controls.Add(new Label { Text = "저장 비밀번호", Dock = DockStyle.Fill, TextAlign = ContentAlignment.MiddleLeft }, 0, 0);
        root.Controls.Add(input, 0, 1);
        root.Controls.Add(buttons, 0, 2);
        form.Controls.Add(root);
        form.AcceptButton = ok;
        form.CancelButton = cancel;

        if (form.ShowDialog(this) != DialogResult.OK)
        {
            return false;
        }

        var expectedPassword = EnvFile.Get("EDIT_PASSWORD"); // 보안
        if (!string.IsNullOrWhiteSpace(expectedPassword) && input.Text == expectedPassword)
        {
            return true;
        }

        MessageBox.Show(this, "비밀번호가 맞지 않습니다.", title, MessageBoxButtons.OK, MessageBoxIcon.Warning);
        return false;
    }

    private static void StyleToolbarButton(Button button)
    {
        button.Height = 30;
        button.Margin = new Padding(4, 9, 4, 4);
    }

    private static void ApplyStatusSplitRatio(SplitContainer split)
    {
        if (split.Width <= 0)
        {
            return;
        }

        var target = (int)(split.Width * 0.47);
        var minLeft = Math.Min(560, split.Width - 420);
        var minRight = 620;
        target = Math.Max(minLeft, Math.Min(target, split.Width - minRight));
        if (target > 0 && target < split.Width)
        {
            split.SplitterDistance = target;
        }
    }

    private static void RemoveEmptyValueColumns(DataTable table)
    {
        if (table.Rows.Count == 0)
        {
            return;
        }

        var remove = new List<DataColumn>();
        foreach (DataColumn column in table.Columns)
        {
            var hasValue = false;
            foreach (DataRow row in table.Rows)
            {
                if (!string.IsNullOrWhiteSpace(Convert.ToString(row[column])))
                {
                    hasValue = true;
                    break;
                }
            }

            if (!hasValue)
            {
                remove.Add(column);
            }
        }

        foreach (var column in remove)
        {
            table.Columns.Remove(column);
        }
    }

    private void BindFlowGrid(string key, DataTable table)
    {
        _grids[key].DataSource = table;
        if (_grids[key].Parent is Control section)
        {
            section.Visible = table.Rows.Count > 0 && table.Columns.Count > 0;
            ResizeProductionInfoGrid(section);
        }
    }

    private static DataTable SelectColumns(DataTable source, params string[] columns)
    {
        var table = new DataTable();
        foreach (var column in columns)
        {
            if (source.Columns.Contains(column))
            {
                table.Columns.Add(column, typeof(string));
            }
        }

        foreach (DataRow sourceRow in source.Rows)
        {
            var row = table.NewRow();
            foreach (DataColumn column in table.Columns)
            {
                row[column.ColumnName] = Convert.ToString(sourceRow[column.ColumnName]) ?? "";
            }
            table.Rows.Add(row);
        }

        return table;
    }

    private void ShowNewSparepartAlarms(DataTable table)
    {
        if (!table.Columns.Contains("station") || !table.Columns.Contains("sparepart") || !table.Columns.Contains("type_alarm"))
        {
            return;
        }

        var messages = new List<string>();
        foreach (DataRow row in table.Rows)
        {
            var key = SparepartAlarmKey(row);
            if (!_seenSparepartAlarmKeys.Add(key))
            {
                continue;
            }

            if (!_sparepartAlarmBaselineLoaded)
            {
                continue;
            }

            var station = Convert.ToString(row["station"])?.Trim() ?? "";
            var sparepart = Convert.ToString(row["sparepart"])?.Trim() ?? "";
            var type = Convert.ToString(row["type_alarm"])?.Trim() ?? "";
            var message = type switch
            {
                "권고" => $"{station}, {sparepart} 교체 권고 드립니다.",
                "긴급" => $"{station}, {sparepart} 교체 긴급 합니다.",
                "교체" => $"{station}, {sparepart} 교체 타이밍이 지났습니다.",
                _ => null,
            };

            if (!string.IsNullOrWhiteSpace(message))
            {
                messages.Add(message);
            }
        }

        _sparepartAlarmBaselineLoaded = true;
        if (messages.Count > 0)
        {
            MessageBox.Show(this, string.Join(Environment.NewLine, messages), "Sparepart 교체 알람", MessageBoxButtons.OK, MessageBoxIcon.Warning);
        }
    }

    private static string SparepartAlarmKey(DataRow row)
    {
        static string Value(DataRow row, string column) => row.Table.Columns.Contains(column)
            ? Convert.ToString(row[column])?.Trim() ?? ""
            : "";

        return string.Join("|",
            Value(row, "end_day"),
            Value(row, "end_time"),
            Value(row, "station"),
            Value(row, "sparepart"),
            Value(row, "type_alarm"),
            Value(row, "amount"));
    }

    private static bool IsFlowGrid(string key) => key is
        "oee_total" or
        "oee_line" or
        "oee_station" or
        "planned_stop" or
        "non_time" or
        "final_amount" or
        "pass_percent" or
        "fct_fail_1" or
        "fct_fail_2" or
        "fct_fail_3" or
        "alarm_record" or
        "pd_board" or
        "worst_case" or
        "afa_wasted" or
        "mes_wasted";

    private static Label SectionLabel(string text) => new()
    {
        Text = text,
        Dock = DockStyle.Fill,
        TextAlign = ContentAlignment.MiddleLeft,
        Font = new Font(SystemFonts.DefaultFont, FontStyle.Bold),
        Padding = new Padding(2, 2, 0, 0),
    };

    private Control WorkerHeader()
    {
        var panel = new TableLayoutPanel
        {
            Dock = DockStyle.Fill,
            ColumnCount = 2,
            RowCount = 1,
        };
        panel.ColumnStyles.Add(new ColumnStyle(SizeType.Percent, 100));
        panel.ColumnStyles.Add(new ColumnStyle(SizeType.Absolute, 170));
        panel.Controls.Add(SectionLabel("작업자 정보"), 0, 0);

        var buttons = new FlowLayoutPanel
        {
            Dock = DockStyle.Fill,
            FlowDirection = FlowDirection.RightToLeft,
            WrapContents = false,
        };
        var save = new Button { Text = "저장", Width = 72, Height = 30, Margin = new Padding(3, 3, 0, 3) };
        save.Click += async (_, _) => await SaveWorkerInfoAsync();
        var add = new Button { Text = "+", Width = 42, Height = 30, Margin = new Padding(3, 3, 0, 3) };
        add.Click += (_, _) => AddWorkerInfoRow();
        buttons.Controls.Add(save);
        buttons.Controls.Add(add);
        panel.Controls.Add(buttons, 1, 0);
        return panel;
    }

    private Control WorkerPanel()
    {
        var root = new TableLayoutPanel
        {
            Dock = DockStyle.Fill,
            ColumnCount = 1,
            RowCount = 3,
            Padding = new Padding(0),
        };
        root.RowStyles.Add(new RowStyle(SizeType.Absolute, 20));
        root.RowStyles.Add(new RowStyle(SizeType.Absolute, 30));
        root.RowStyles.Add(new RowStyle(SizeType.Percent, 100));

        root.Controls.Add(SectionLabel("작업자 정보"), 0, 0);

        var buttons = new FlowLayoutPanel
        {
            Dock = DockStyle.Fill,
            FlowDirection = FlowDirection.RightToLeft,
            WrapContents = false,
        };
        var tinyButtonFont = new Font(SystemFonts.DefaultFont.FontFamily, 8.25f, FontStyle.Regular);
        var save = new Button { Text = "저장", Width = 58, Height = 22, Margin = new Padding(3, 1, 0, 1), Font = tinyButtonFont };
        save.Click += async (_, _) => await SaveWorkerInfoAsync();
        var add = new Button { Text = "+", Width = 34, Height = 22, Margin = new Padding(3, 1, 0, 1), Font = tinyButtonFont };
        add.Click += (_, _) => AddWorkerInfoRow();
        buttons.Controls.Add(save);
        buttons.Controls.Add(add);

        root.Controls.Add(buttons, 0, 1);
        root.Controls.Add(CreateGrid("worker_info", readOnly: false), 0, 2);
        return root;
    }

    private static Label Label(string text) => new()
    {
        Text = text,
        AutoSize = true,
        Padding = new Padding(12, 12, 4, 0),
    };
}
