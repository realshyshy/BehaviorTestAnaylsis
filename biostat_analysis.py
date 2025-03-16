import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import pandas as pd
import numpy as np
from scipy import stats
from scipy.stats import shapiro, levene, kruskal, mannwhitneyu
import statsmodels.api as sm
from statsmodels.formula.api import ols
from statsmodels.stats.multicomp import pairwise_tukeyhsd
from pathlib import Path
from datetime import datetime
import string
import threading
import re
import traceback
import os
import scipy
from packaging.version import parse

# 添加依赖检查
try:
    from scikit_posthocs import posthoc_dunn
except ImportError as e:
    raise RuntimeError("需要安装scikit-posthocs库：pip install scikit-posthocs") from e

class StatsAnalysisApp:
    def __init__(self, master):
        self.master = master
        master.title("生物医学统计分析工具 v1.1")  # 版本更新
        master.geometry("800x600")  # 调整窗口大小
        
        # 依赖检查
        self.check_dependencies()
        
        # 样式配置
        self.setup_style()
        
        # 主界面
        self.setup_ui()
        
    def check_dependencies(self):
        """检查必要依赖库"""
        try:
            import scikit_posthocs  # noqa
        except ImportError:
            messagebox.showerror("依赖错误", 
                "请先安装 scikit-posthocs 库：\npip install scikit-posthocs")
            self.master.destroy()
        
        """检查依赖版本"""
        min_versions = {
            'scipy': '1.6.0',
            'statsmodels': '0.12.0',
            'pandas': '1.2.0'
        }
        
        current_versions = {
            'scipy': scipy.__version__,
            'statsmodels': sm.__version__,
            'pandas': pd.__version__
        }
        
        warnings = []
        for lib, ver in current_versions.items():
            if parse(ver) < parse(min_versions[lib]):
                warnings.append(f"{lib} 版本过低 ({ver} < {min_versions[lib]})")
        
        if warnings:
            msg = "以下依赖版本可能过低：\n" + "\n".join(warnings)
            self.log_message(msg, "warning")

    def setup_style(self):
        """界面样式配置"""
        style = ttk.Style()
        style.configure("TFrame", padding=10)
        style.configure("TButton", padding=5, font=('微软雅黑', 10))
        style.configure("TLabel", padding=5, font=('微软雅黑', 10))
        style.configure("Header.TLabel", font=('微软雅黑', 12, 'bold'))

    def setup_ui(self):
        """界面布局"""
        main_frame = ttk.Frame(self.master)
        main_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        # 文件选择
        self.setup_file_controls(main_frame)
        
        # 分组列设置
        self.setup_group_control(main_frame)
        
        # 输出目录
        self.setup_output_controls(main_frame)
        
        # 进度条
        self.setup_progress_bar(main_frame)
        
        # 日志
        self.setup_log_view(main_frame)
        
        # 控制按钮
        self.setup_action_buttons(main_frame)

    def setup_file_controls(self, parent):
        """文件选择组件"""
        frame = ttk.Frame(parent)
        frame.pack(fill=tk.X, pady=5)
        
        ttk.Label(frame, text="数据文件:", style="Header.TLabel").pack(side=tk.LEFT)
        self.file_entry = ttk.Entry(frame, width=60)
        self.file_entry.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=5)
        ttk.Button(frame, text="浏览...", command=self.select_file).pack(side=tk.LEFT)

    def setup_group_control(self, parent):
        """分组列设置"""
        frame = ttk.Frame(parent)
        frame.pack(fill=tk.X, pady=5)
        
        ttk.Label(frame, text="分组列名:").pack(side=tk.LEFT)
        self.group_entry = ttk.Entry(frame, width=20)
        self.group_entry.pack(side=tk.LEFT, padx=5)
        self.group_entry.insert(0, "group")

    def setup_output_controls(self, parent):
        """输出目录组件"""
        frame = ttk.Frame(parent)
        frame.pack(fill=tk.X, pady=5)
        
        ttk.Label(frame, text="输出目录:").pack(side=tk.LEFT)
        self.output_entry = ttk.Entry(frame, width=60)
        self.output_entry.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=5)
        ttk.Button(frame, text="浏览...", command=self.select_output).pack(side=tk.LEFT)

    def setup_progress_bar(self, parent):
        """进度条组件"""
        self.progress = ttk.Progressbar(parent, mode='determinate')
        self.progress.pack(fill=tk.X, pady=10)

    def setup_log_view(self, parent):
        """日志组件"""
        log_frame = ttk.Frame(parent)
        log_frame.pack(fill=tk.BOTH, expand=True)
        
        ttk.Label(log_frame, text="运行日志:", style="Header.TLabel").pack(anchor=tk.W)
        
        self.log_text = tk.Text(log_frame, wrap=tk.WORD, state='disabled')
        vsb = ttk.Scrollbar(log_frame, command=self.log_text.yview)
        self.log_text.configure(yscrollcommand=vsb.set)
        
        self.log_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        vsb.pack(side=tk.RIGHT, fill=tk.Y)

    def setup_action_buttons(self, parent):
        """操作按钮"""
        frame = ttk.Frame(parent)
        frame.pack(pady=10)
        
        self.start_btn = ttk.Button(frame, text="开始分析", command=self.start_analysis)
        self.start_btn.pack(side=tk.LEFT, padx=5)
        
        ttk.Button(frame, text="退出", command=self.master.quit).pack(side=tk.LEFT)

    # 以下是事件处理方法
    def select_file(self):
        file_path = filedialog.askopenfilename(
            filetypes=[("Excel文件", "*.xlsx"), ("CSV文件", "*.csv"), ("所有文件", "*.*")]
        )
        self.file_entry.delete(0, tk.END)
        self.file_entry.insert(0, file_path)

    def select_output(self):
        dir_path = filedialog.askdirectory()
        self.output_entry.delete(0, tk.END)
        self.output_entry.insert(0, dir_path)

    def log_message(self, message, level="info"):
        """线程安全的日志记录"""
        def update_log():
            tag = {
                "info": ("black",),
                "warning": ("orange", "bold"),
                "error": ("red", "bold")
            }.get(level, ("black",))
            
            self.log_text.configure(state='normal')
            self.log_text.insert(tk.END, message + "\n", tag)
            self.log_text.see(tk.END)
            self.log_text.configure(state='disabled')
        
        self.master.after(0, update_log)

    def start_analysis(self):
        """启动分析线程"""
        file_path = self.file_entry.get()
        output_dir = self.output_entry.get()
        group_col = self.group_entry.get()

        if not file_path:
            messagebox.showerror("错误", "请先选择数据文件")
            return

        if not output_dir:
            messagebox.showerror("错误", "请选择输出目录")
            return

        # 禁用按钮防止重复提交
        self.start_btn.config(state=tk.DISABLED)
        self.progress["value"] = 0

        def analysis_thread():
            try:
                analyzer = DataAnalyzer(file_path, group_col, output_dir)
                analyzer.set_callback(
                    progress=lambda x: self.master.after(0, lambda: self.progress.config(value=x)),
                    log=self.log_message
                )
                analyzer.full_analysis()
                self.log_message("分析完成！结果已保存至：" + output_dir, "info")
            except Exception as e:
                self.log_message(f"分析错误：{str(e)}", "error")
                messagebox.showerror("分析错误", str(e))
            finally:
                self.master.after(0, lambda: self.start_btn.config(state=tk.NORMAL))

        threading.Thread(target=analysis_thread, daemon=True).start()

class DataAnalyzer:
    def __init__(self, file_path, group_col, output_dir):
        self.file_path = file_path
        self.group_col = group_col
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        self._progress_callback = None
        self._log_callback = None

    def set_callback(self, progress=None, log=None):
        """设置回调函数"""
        self._progress_callback = progress
        self._log_callback = log

    def full_analysis(self):
        """完整的分析流程"""
        self._log(f"输出目录验证: {'存在' if self.output_dir.exists() else '不存在'}")
        self._log(f"输出目录权限: {'可写' if os.access(self.output_dir, os.W_OK) else '只读'}")
        try:
            long_data, value_cols = self.load_and_preprocess()
            total = len(value_cols)
            
            for idx, col in enumerate(value_cols):
                try:
                    self._update_progress((idx+1)/total*100)
                    subset = long_data[long_data['variable'] == col]
                    
                    if self.skip_analysis(subset):
                        continue
                    
                    normality = self.check_normality(subset)
                    test_type = self.select_stat_test(subset, normality)
                    test_result = self.perform_test(subset, test_type)
                    posthoc = self.posthoc_analysis(subset, test_result)
                    
                    self.generate_report(col, subset, test_result, posthoc, normality)
                    
                except Exception as e:
                    self._log(f"跳过变量 {col} 的分析，原因：{str(e)}", "warning")
                    continue
                    
        except Exception as e:
            raise RuntimeError(f"全局分析失败: {str(e)}")

    def _log(self, message, level="info"):
        """记录日志"""
        if self._log_callback:
            self._log_callback(message, level)

    def _update_progress(self, value):
        """更新进度"""
        if self._progress_callback:
            self._progress_callback(value)

    def skip_analysis(self, data):
        """判断是否跳过分析"""
        if data['value'].nunique() == 1:
            self._log(f"变量 {data['variable'].iloc[0]} 所有取值相同，跳过分析", "warning")
            return True
            
        group_counts = data.groupby(self.group_col).size()
        if group_counts.min() < 3:
            self._log(f"{data['variable'].iloc[0]} 存在分组样本量小于3，跳过分析", "warning")
            return True
            
        return False

    def load_and_preprocess(self):
        try:
            # 读取数据
            data = pd.read_excel(self.file_path) if self.file_path.endswith('.xlsx') \
                else pd.read_csv(self.file_path)
            
            self._log(f"原始数据列名: {data.columns.tolist()}")
            
            # 清理列名（保留原始大小写）
            data.columns = [
                re.sub(r'[^\w]', '_', col)
                .replace('__', '_')
                .strip('_')
                for col in data.columns
            ]
            self._log(f"清理后列名: {data.columns.tolist()}")
            
            # 查找实际分组列名（大小写不敏感）
            actual_group_col = next(
                (col for col in data.columns if col.lower() == self.group_col.lower()),
                None
            )
            if not actual_group_col:
                from difflib import get_close_matches
                suggestions = get_close_matches(self.group_col, data.columns, n=3, cutoff=0.6)
                err_msg = f"分组列 {self.group_col} 不存在，可用列：{data.columns.tolist()}"
                if suggestions:
                    err_msg += f"\n可能匹配的列：{suggestions}"
                raise ValueError(err_msg)
            
            # 更新分组列
            self.group_col = actual_group_col
            self._log(f"实际使用分组列: {self.group_col}")
                
            # 验证分组列存在
            if self.group_col not in data.columns:
                available = [col for col in data.columns if col != 'variable']
                raise ValueError(
                    f"分组列 {self.group_col} 不存在，可用列：{available}\n"
                    f"当前所有列：{data.columns.tolist()}"
                )
                
            # 验证数值列
            value_cols = [col for col in data.columns if col != self.group_col]
            for col in value_cols:
                if not pd.api.types.is_numeric_dtype(data[col]):
                    invalid_values = data[col][pd.to_numeric(data[col], errors='coerce').isna()]
                    raise ValueError(
                        f"数值列 {col} 包含非数值数据\n"
                        f"无效值样例：\n{invalid_values.head().to_string()}"
                    )
            
            # 转换长格式
            long_data = pd.melt(
                data, 
                id_vars=[self.group_col], 
                value_vars=value_cols,
                var_name='variable',
                value_name='value'
            )
            
            return long_data, value_cols
            
        except Exception as e:
            raise RuntimeError(f"数据加载失败: {str(e)}")

    def check_normality(self, data):
        """正态性检验"""
        normality = {}
        for group in data[self.group_col].unique():
            sample = data[data[self.group_col] == group]['value'].dropna()
            if len(sample) < 3:
                raise ValueError(f"分组 {group} 样本量不足（n={len(sample)}）")
            _, p = shapiro(sample)
            normality[group] = p
        return normality

    def select_stat_test(self, data, normality):
        """选择统计检验方法"""
        normal = all(p > 0.05 for p in normality.values())
        
        if normal:
            groups = [data[data[self.group_col] == g]['value'].dropna() 
                     for g in data[self.group_col].unique()]
            _, homo_p = levene(*groups)
            return 'anova' if homo_p > 0.05 else 'nonparametric'
        return 'nonparametric'

    def perform_test(self, data, test_type):
        """执行统计检验"""
        groups = data[self.group_col].unique()
        
        if test_type == 'anova':
            model = ols(f'value ~ C({self.group_col})', data=data).fit()
            anova_table = sm.stats.anova_lm(model, typ=2)
            return {
                'test': 'ANOVA',
                'stat': anova_table.loc['C('+self.group_col+')', 'F'],  # 使用loc精确索引
                'pval': anova_table.loc['C('+self.group_col+')', 'PR(>F)'],  # 使用loc精确索引
                'groups': groups
            }
        else:
            if len(groups) == 2:
                g1 = data[data[self.group_col]==groups[0]]['value'].dropna()
                g2 = data[data[self.group_col]==groups[1]]['value'].dropna()
                stat, p = mannwhitneyu(g1, g2)
                return {
                    'test': 'Mann-Whitney U',
                    'stat': stat,
                    'pval': p,
                    'groups': groups
                }
            else:
                samples = [data[data[self.group_col]==g]['value'].dropna() for g in groups]
                stat, p = kruskal(*samples)
                return {
                    'test': 'Kruskal-Wallis',
                    'stat': stat,
                    'pval': p,
                    'groups': groups
                }

    def posthoc_analysis(self, data, test_result):
        """事后检验（修正版）"""
        if test_result['pval'] > 0.05:
            return {'method': '无', 'result': None}
        
        if test_result['test'] == 'ANOVA':
            tukey = pairwise_tukeyhsd(data['value'], data[self.group_col])
            return {
                'method': 'Tukey HSD',
                'result': tukey.summary().as_html() if hasattr(tukey, 'summary') else str(tukey)
            }
        else:
            try:
                dunn = posthoc_dunn(
                    data,
                    group_col=self.group_col,
                    val_col='value',
                    p_adjust='bonferroni'
                )
                return {
                    'method': 'Dunn检验（Bonferroni校正）',
                    'result': dunn.style.format("{:.4f}").to_html()
                }
            except Exception as e:
                self._log(f"Dunn检验失败: {str(e)}", "error")
                return {'method': 'Dunn检验失败', 'result': str(e)}

    # 修改generate_report方法
    def generate_report(self, variable_name, data, test_result, posthoc, normality):
        """生成分析报告（完整错误处理）"""
        try:
            # 清理文件名特殊字符
            safe_name = re.sub(r'[\\/*?:"<>|]', "_", variable_name)
            filename = self.output_dir / f"{safe_name.replace(' ', '_')}_report.md"
            
            # 生成报告内容
            content = self._build_report_content(variable_name, data, test_result, posthoc, normality)
            
            # 验证输出目录
            if not self.output_dir.exists():
                self.output_dir.mkdir(parents=True, exist_ok=True)
            
            # 写入文件（添加编码处理）
            with open(filename, 'w', encoding='utf-8') as f:
                f.write(content)
                
            self._log(f"成功生成报告: {filename.name}", "info")
            
        except PermissionError as pe:
            error_msg = f"文件权限错误: {str(pe)}\n请检查目录写入权限: {self.output_dir}"
            self._log(error_msg, "error")
        except Exception as e:
            error_msg = f"生成报告失败: {str(e)}\n{traceback.format_exc()}"
            self._log(error_msg, "error")

    def _build_report_content(self, variable_name, data, test_result, posthoc, normality):
        """确保返回有效字符串内容"""
        try:
            # 获取描述性统计
            stats_df = self._get_descriptive_stats(data)
            if stats_df.empty:
                return f"# 错误报告 - {variable_name}\n无法生成描述性统计数据"

            # 生成报告各部分内容
            stats_table = stats_df.to_markdown(index=False)
            warnings = self._get_data_warnings(stats_df)
            normality_info = self._format_normality(normality)
            homo_info = self._get_homogeneity_info(data, test_result)
            posthoc_info = self._format_posthoc(posthoc)

            # 组装完整报告
            return f"""# 统计分析报告 - {variable_name}
**生成时间**: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

## 数据基本信息
### 描述性统计
{stats_table}

### 数据质量检查
{warnings}

## 分析流程
1. 数据加载与预处理
2. 正态性检验（Shapiro-Wilk）
3. 方差齐性检验（Levene）
4. 主效应分析（{test_result['test']}）
5. 事后分析（{posthoc['method'] if posthoc else '无'}）

## 详细结果
### 正态性检验
{normality_info}

### 方差齐性分析
{homo_info}

### 主效应分析
- 检验方法：{test_result['test']}
- 统计量：{test_result['stat']:.4f}
- P值：{test_result['pval']:.4f}
- 结论：{self._get_conclusion(test_result['pval'])}

## 事后分析
{posthoc_info}

## 软件信息
- 分析工具：Python SciPy {scipy.__version__}
- 统计模型：statsmodels {sm.__version__}
- 数据处理：pandas {pd.__version__}
- 报告版本：1.4
- 生成时间：{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
"""
        except Exception as e:
            error_msg = f"报告内容生成失败: {str(e)}\n{traceback.format_exc()}"
            self._log(error_msg, "error")
            return f"# 错误报告 - {variable_name}\n{error_msg}"

    def _get_descriptive_stats(self, data):
        """获取描述性统计数据（兼容旧版pandas）"""
        try:
            grouped = data.groupby([self.group_col, 'variable'])['value']
            
            # 定义聚合函数字典
            agg_funcs = {
                'count': 'count',
                'mean': lambda x: x.mean(skipna=False),
                'std': lambda x: x.std(ddof=1, skipna=False),
                'min': 'min',
                'median': 'median',
                'max': 'max'
            }
            
            # 执行聚合
            stats_df = grouped.agg(**agg_funcs).reset_index()
            
            # 重命名为中文列名
            stats_df.columns = [
                self.group_col, 'variable', 
                '样本量', '均值', '标准差', 
                '最小值', '中位数', '最大值'
            ]
            
            return stats_df
            
        except Exception as e:
            self._log(f"描述统计失败: {str(e)}", "error")
            return pd.DataFrame()
        
    def _get_data_warnings(self, stats_df):
        """获取数据警告（更新列名验证）"""
        try:
            warnings = []
            required_columns = ['样本量']
            if not all(col in stats_df.columns for col in required_columns):
                missing = [col for col in required_columns if col not in stats_df.columns]
                raise KeyError(f"缺少必要列: {missing}")

            if (stats_df['样本量'] < 3).any():
                warnings.append("⚠️ 存在分组样本量<3，结果不可靠")
            elif (stats_df['样本量'] < 5).any():
                warnings.append("⚠️ 存在分组样本量<5，建议谨慎解释结果")
                
            return "\n".join(warnings) if warnings else "无严重数据质量问题"
            
        except KeyError as e:
            self._log(f"数据警告检查失败: {str(e)}", "error")
            return "数据质量检查失败：列名不匹配"

    def _format_normality(self, normality):
        """格式化正态性检验结果"""
        return "\n".join([f"- {group}: p={p:.4f} ({'正态' if p>0.05 else '非正态'})" 
                        for group, p in normality.items()])

    def _get_homogeneity_info(self, data, test_result):
        """获取方差齐性信息"""
        if test_result['test'] == 'ANOVA':
            groups = [data[data[self.group_col] == g]['value'] 
                     for g in data[self.group_col].unique()]
            _, homo_p = levene(*groups)
            return f"Levene检验 p值={homo_p:.4f} ({'齐性' if homo_p>0.05 else '不齐'})"
        return "因未通过正态性检验，未进行方差齐性分析"

    def _get_conclusion(self, pval):
        """获取结论描述"""
        if pval < 0.001:
            return "差异极显著 (p < 0.001)"
        elif pval < 0.01:
            return "差异非常显著 (p < 0.01)"
        elif pval < 0.05:
            return "差异显著 (p < 0.05)"
        else:
            return "差异不显著 (p ≥ 0.05)"

    def _format_posthoc(self, posthoc):
        """格式化事后检验结果"""
        if not posthoc or not posthoc['result']:
            return "无需进行事后检验"
        return f"```html\n{posthoc['result']}\n```"

if __name__ == "__main__":
    root = tk.Tk()
    app = StatsAnalysisApp(root)
    root.mainloop()