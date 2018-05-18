using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Configuration;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.ServiceProcess;
using System.Text;
using System.Threading.Tasks;
using System.Timers;

namespace KSoftDBBackupService
{
    public partial class KSoftDBBService : ServiceBase
    {
        System.Timers.Timer _dailyTimer;
        DateTime _scheduleTime;
        String  _dailyBackupTime= ConfigurationManager.AppSettings["DailyBackupTime"];
        String _MonthlyBackupDate = ConfigurationManager.AppSettings["MonthlyBackupDate"];

        public KSoftDBBService()
        {
            InitializeComponent();
            _dailyTimer = new System.Timers.Timer();

            DateTime dailyBckTime = Convert.ToDateTime(_dailyBackupTime);
            Logger.log("BACKUP TIME : " + dailyBckTime.ToString("HH:mm:ss"));
            _scheduleTime = DateTime.Today.AddDays(1).AddHours(dailyBckTime.Hour).AddMinutes(dailyBckTime.Minute).AddSeconds(dailyBckTime.Second); // Schedule to run once a day at 1:00 a.m.
        }

        protected override void OnStart(string[] args)
        {
            Logger.log("Service Started at " + DateTime.Now);
            _dailyTimer.Enabled = true;          
            _dailyTimer.Interval = _scheduleTime.Subtract(DateTime.Now).TotalSeconds * 1000;
            _dailyTimer.Elapsed += new System.Timers.ElapsedEventHandler(DailyBackup);
            DailyBackupProcessor.InstantFullBackup();
        }

        private void DailyBackup(object sender, ElapsedEventArgs e)
        {
            Logger.log("Timer fired at " + DateTime.Now);
            if (_dailyTimer.Interval != 24 * 60 * 60 * 1000)
            {
                _dailyTimer.Interval = 24 * 60 * 60 * 1000;
            }
            if(Convert.ToInt32(_MonthlyBackupDate) == DateTime.Now.Day)
            {
                DailyBackupProcessor.CreateFullBackup(Convert.ToInt32(DateTime.Now.Month));
                Logger.log("Create full backup");
            }
            else
            {
                DailyBackupProcessor.CreateIncrementalBackup(Convert.ToInt32(DateTime.Now.Month));
                Logger.log("Create increamental backup");
            }
            
        }

        protected override void OnStop()
        {
            _dailyTimer.Enabled = false;
            Logger.log("Service stopped at " + DateTime.Now);
        }
    }
}
