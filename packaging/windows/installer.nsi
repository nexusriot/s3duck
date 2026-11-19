; S3 Duck -- Windows installer (NSIS 3).
;
; Built by `make win-installer`, in the installer-build stage of
; packaging/windows/Dockerfile:
;
;   makensis -DVERSION=x.y.z -DOUTFILE=... installer.nsi
;
; beside app\ (PyInstaller's one-folder build), ducky.ico and LICENSE.
;
; The one decision it asks for is who the program is installed for:
;
;   only me       %LOCALAPPDATA%\Programs\S3 Duck, registered under HKCU.
;                 Needs no administrator rights.
;   anyone        %ProgramFiles%\S3 Duck, registered under HKLM.
;
; MultiUser.nsh does that, with the execution level "Highest": an
; administrator is elevated (one UAC prompt, at start) and offered both; a
; standard user is never prompted and is offered "only me", with the other
; choice greyed out. Unattended:
;
;   s3duck-x.y.z-setup.exe /S /CurrentUser
;   s3duck-x.y.z-setup.exe /S /AllUsers [/D=C:\Some\Folder]
;
; Settings and saved profiles (HKCU\Software\s3duck, %APPDATA%) belong to the
; user, not to the installation: neither install nor uninstall touches them.

Unicode true
SetCompressor /SOLID lzma

!ifndef VERSION
  !error "pass -DVERSION=x.y.z"
!endif
!ifndef OUTFILE
  !define OUTFILE "s3duck-${VERSION}-setup.exe"
!endif

!define APP_NAME      "S3 Duck"
!define APP_EXE       "s3duck.exe"
!define APP_PUBLISHER "Vladislav Ananyev"
; Where Windows lists installed programs; HKLM or HKCU by install mode.
!define UNINST_KEY    "Software\Microsoft\Windows\CurrentVersion\Uninstall\S3Duck"

Name "${APP_NAME}"
OutFile "${OUTFILE}"
BrandingText "${APP_NAME} ${VERSION}"

VIProductVersion "${VERSION}.0"
VIAddVersionKey "ProductName"     "${APP_NAME}"
VIAddVersionKey "ProductVersion"  "${VERSION}"
VIAddVersionKey "FileVersion"     "${VERSION}"
VIAddVersionKey "FileDescription" "${APP_NAME} installer"
VIAddVersionKey "LegalCopyright"  "${APP_PUBLISHER}"

; ---- who it is installed for ------------------------------------------------

!define MULTIUSER_EXECUTIONLEVEL Highest
!define MULTIUSER_MUI
!define MULTIUSER_INSTALLMODE_COMMANDLINE
!define MULTIUSER_USE_PROGRAMFILES64
!define MULTIUSER_INSTALLMODE_INSTDIR "${APP_NAME}"
; An upgrade starts from the mode and the folder of the copy already there.
!define MULTIUSER_INSTALLMODE_DEFAULT_REGISTRY_KEY       "${UNINST_KEY}"
!define MULTIUSER_INSTALLMODE_DEFAULT_REGISTRY_VALUENAME "InstallMode"
!define MULTIUSER_INSTALLMODE_INSTDIR_REGISTRY_KEY       "${UNINST_KEY}"
!define MULTIUSER_INSTALLMODE_INSTDIR_REGISTRY_VALUENAME "InstallLocation"

!include "MultiUser.nsh"
!include "MUI2.nsh"
!include "x64.nsh"
!include "FileFunc.nsh"

; ---- pages ------------------------------------------------------------------

!define MUI_ICON   "ducky.ico"
!define MUI_UNICON "ducky.ico"
!define MUI_ABORTWARNING

!insertmacro MUI_PAGE_LICENSE "LICENSE"
!insertmacro MULTIUSER_PAGE_INSTALLMODE
!insertmacro MUI_PAGE_COMPONENTS
!insertmacro MUI_PAGE_DIRECTORY
!insertmacro MUI_PAGE_INSTFILES
!insertmacro MUI_PAGE_FINISH

!insertmacro MUI_UNPAGE_CONFIRM
!insertmacro MUI_UNPAGE_INSTFILES

!insertmacro MUI_LANGUAGE "English"

; ---- install ----------------------------------------------------------------

Function .onInit
  ; The app is a 64-bit build; say so here rather than let it fail to start.
  ${IfNot} ${RunningX64}
    MessageBox MB_ICONSTOP "${APP_NAME} needs 64-bit Windows."
    Abort
  ${EndIf}
  ; This installer is a 32-bit program. Without this its HKLM writes land
  ; in the WOW6432Node view, as if the app were 32-bit too.
  SetRegView 64
  !insertmacro MULTIUSER_INIT
FunctionEnd

Section "${APP_NAME}" SecApp
  SectionIn RO

  ; A previous version's files first: a module the new build no longer
  ; ships must not be left behind to be imported by it.
  RMDir /r "$INSTDIR\_internal"

  SetOutPath "$INSTDIR"
  File /r "app\*"
  WriteUninstaller "$INSTDIR\uninstall.exe"

  ; $SMPROGRAMS follows the mode: every user's Start menu, or this one's.
  CreateShortcut "$SMPROGRAMS\${APP_NAME}.lnk" "$INSTDIR\${APP_EXE}"

  WriteRegStr SHCTX "${UNINST_KEY}" "DisplayName"     "${APP_NAME}"
  WriteRegStr SHCTX "${UNINST_KEY}" "DisplayVersion"  "${VERSION}"
  WriteRegStr SHCTX "${UNINST_KEY}" "Publisher"       "${APP_PUBLISHER}"
  WriteRegStr SHCTX "${UNINST_KEY}" "DisplayIcon"     "$INSTDIR\${APP_EXE}"
  WriteRegStr SHCTX "${UNINST_KEY}" "InstallLocation" "$INSTDIR"
  WriteRegStr SHCTX "${UNINST_KEY}" "InstallMode"     "$MultiUser.InstallMode"
  WriteRegStr SHCTX "${UNINST_KEY}" "UninstallString" \
      '"$INSTDIR\uninstall.exe" /$MultiUser.InstallMode'
  WriteRegStr SHCTX "${UNINST_KEY}" "QuietUninstallString" \
      '"$INSTDIR\uninstall.exe" /$MultiUser.InstallMode /S'
  WriteRegDWORD SHCTX "${UNINST_KEY}" "NoModify" 1
  WriteRegDWORD SHCTX "${UNINST_KEY}" "NoRepair" 1
  ${GetSize} "$INSTDIR" "/S=0K" $0 $1 $2
  IntFmt $0 "0x%08X" $0
  WriteRegDWORD SHCTX "${UNINST_KEY}" "EstimatedSize" "$0"
SectionEnd

Section /o "Desktop shortcut" SecDesktop
  CreateShortcut "$DESKTOP\${APP_NAME}.lnk" "$INSTDIR\${APP_EXE}"
SectionEnd

!insertmacro MUI_FUNCTION_DESCRIPTION_BEGIN
  !insertmacro MUI_DESCRIPTION_TEXT ${SecApp} "The program and a Start menu shortcut."
  !insertmacro MUI_DESCRIPTION_TEXT ${SecDesktop} "A shortcut on the desktop."
!insertmacro MUI_FUNCTION_DESCRIPTION_END

; ---- uninstall --------------------------------------------------------------

Function un.onInit
  SetRegView 64
  !insertmacro MULTIUSER_UNINIT
  ; Started by hand, with no /AllUsers or /CurrentUser: the copy that
  ; registered this very folder says which one it is.
  ReadRegStr $0 HKCU "${UNINST_KEY}" "InstallLocation"
  ${If} $0 == $INSTDIR
    Call un.MultiUser.InstallMode.CurrentUser
  ${Else}
    ReadRegStr $0 HKLM "${UNINST_KEY}" "InstallLocation"
    ${If} $0 == $INSTDIR
      Call un.MultiUser.InstallMode.AllUsers
    ${EndIf}
  ${EndIf}
FunctionEnd

Section "Uninstall"
  ; Only what the installer put there, by name. The folder is the user's
  ; choice, and a recursive delete of "whatever $INSTDIR is" would take
  ; everything else in it along -- C:\Tools, say.
  RMDir /r "$INSTDIR\_internal"
  Delete "$INSTDIR\${APP_EXE}"
  Delete "$INSTDIR\uninstall.exe"
  RMDir "$INSTDIR"

  Delete "$SMPROGRAMS\${APP_NAME}.lnk"
  Delete "$DESKTOP\${APP_NAME}.lnk"
  DeleteRegKey SHCTX "${UNINST_KEY}"
SectionEnd
