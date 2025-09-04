from django.contrib import admin
from .models import Physiography, ForestSpecies, HDModel, SpeciesHDModelMap, Project


class PhysiographyAdmin(admin.ModelAdmin):
    ordering = ('code',)
    list_display=('name','ecological','code')    
    search_fields = ('code','name','ecological')
     

class ForestSpeciesAdmin(admin.ModelAdmin):
    ordering = ('species_name','code')
    list_display=('species_name','code','species','family','form','nepal','altitude','local_name')    
    search_fields = ('code','species_name','species','family','local_name' )
    list_filter = ('species','family') 

class HDModelAdmin(admin.ModelAdmin):
    ordering = ('code',)
    list_display=('name','code','expression','description')    
    search_fields = ('code','name')

class SpeciesHDModelMapAdmin(admin.ModelAdmin):
    ordering = ('physiography', 'species',)
    list_display = (
        'get_species_code', 
        'get_species_name',
        'hd_model',
        'physiography',
        'get_hd_a',
        'get_hd_b',
        'get_hd_c'
    )
    search_fields = (
        'species__code',
        'species__species_name',
        'hd_model__name',
        'physiography__name'
    )
    list_filter = (
        'hd_model',
        'physiography'
    )
    class Media:
        css = {
            'all': ('admin/css/admin_custom.css',)
        }
    
    # Custom methods for species info
    def get_species_code(self, obj):
        return obj.species.code
    get_species_code.short_description = 'Species Code'
    get_species_code.admin_order_field = 'species__code'
    
    def get_species_name(self, obj):
        return obj.species.species_name
    get_species_name.short_description = 'Species Name'
    get_species_name.admin_order_field = 'species__species_name'
    
    # Custom methods for simplified hd parameter display
    def get_hd_a(self, obj):
        return obj.hd_a
    get_hd_a.short_description = '\u200ba' 
    get_hd_a.admin_order_field = 'hd_a'
    
    def get_hd_b(self, obj):
        return obj.hd_b
    get_hd_b.short_description = '\u200bb'
    get_hd_b.admin_order_field = 'hd_b'
    
    def get_hd_c(self, obj):
        return obj.hd_c
    get_hd_c.short_description = '\u200bc'
    get_hd_c.admin_order_field = 'hd_c'

class ProjectAdmin(admin.ModelAdmin):
    list_display = ('name', 'status', 'current_phase', 'current_step', 'created_by', 'created_date', 'last_modified')
    list_filter = ('status', 'current_phase', 'created_date')
    search_fields = ('name', 'description')
    readonly_fields = ('created_date', 'last_modified')
    fieldsets = (
        ('Basic Information', {
            'fields': ('name', 'description')
        }),
        ('Status & Progress', {
            'fields': ('status', 'current_phase', 'current_step')
        }),
        ('Metadata', {
            'fields': ('created_by', 'created_date', 'last_modified'),
            'classes': ('collapse',)
        }),
    )
    
    def get_queryset(self, request):
        return super().get_queryset(request).select_related('created_by')
    
    def save_model(self, request, obj, form, change):
        if not change:  # Only set created_by on creation
            obj.created_by = request.user
        super().save_model(request, obj, form, change)

# Register your models here.
admin.site.register(Physiography,PhysiographyAdmin)
admin.site.register(ForestSpecies,ForestSpeciesAdmin)
admin.site.register(HDModel,HDModelAdmin)
admin.site.register(SpeciesHDModelMap,SpeciesHDModelMapAdmin)
admin.site.register(Project, ProjectAdmin)